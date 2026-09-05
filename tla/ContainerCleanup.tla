------------------------------ MODULE ContainerCleanup ------------------------------
EXTENDS Integers, Naturals
(***************************************************************************)
(* Formal specification of the replicated container and TTL node cleanup,  *)
(* the second P0 target from the roadmap.                                  *)
(*                                                                         *)
(* Extended nodes (containers and TTL nodes) are reaped by the leader: it  *)
(* scans for nodes whose cleanup time has passed, then proposes a          *)
(* delete_extended entry carrying the czxid and kind it observed at scan   *)
(* time. The apply is a compare-and-set: the node is removed only when     *)
(* its current czxid equals the scanned czxid and its kind matches. This   *)
(* fences the recreate race: if the path was deleted and recreated         *)
(* between scan and commit, the new incarnation has a different czxid and  *)
(* the stale cleanup must be a no-op.                                     *)
(*                                                                         *)
(* The spec mirrors src/server/rocks_store.zig: delete_extended requires   *)
(* the path to exist with the expected czxid and node kind, and containers *)
(* are only scanned while empty.                                          *)
(***************************************************************************)

CONSTANTS MaxClock,     (* bound on the maintenance clock                  *)
          MaxCzxid      (* bound on the create version counter            *)

Kinds == {1, 2}         (* 1 = container, 2 = TTL                         *)

VARIABLES clock,        (* maintenance clock                              *)
          node,         (* -1 (absent) or [czxid, kind, has_children,     *)
                        (*    expires_at]                                 *)
          next_czxid,   (* monotonically increasing create counter        *)
          scan,         (* -1 (none) or the scanned [czxid, kind]         *)
          victim,       (* -1 (none) or the [czxid, kind] actually        *)
                        (*    removed by the last applied cleanup         *)
          lastScan      (* the [czxid, kind] the applied cleanup carried  *)

TypeOK ==
    /\ clock \in 0..MaxClock
    /\ node \in {<<"absent">>} \union
        {<<"node", c, k, h, e>> :
            c \in 1..MaxCzxid, k \in Kinds, h \in {0, 1}, e \in 0..MaxClock + 1}
    /\ next_czxid \in 1..MaxCzxid + 1
    /\ scan \in {<<"noscan">>} \union {<<"scan", c, k>> :
        c \in 1..MaxCzxid, k \in Kinds}
    /\ victim \in {<<"none">>} \union {<<"removed", c, k, ch>> :
        c \in 1..MaxCzxid, k \in Kinds, ch \in {0, 1}}
    /\ lastScan \in {<<"none">>} \union {<<"scan", c, k>> :
        c \in 1..MaxCzxid, k \in Kinds}

Absent == node[1] = "absent"
ScanCzxid == scan[2]
ScanKind == scan[3]
VictimCzxid == victim[2]
VictimKind == victim[3]
Czxid == node[2]
Kind == node[3]
HasChildren == node[4]
ExpiresAt == node[5]

(*************************************************************************)
(* Actions                                                               *)
(*************************************************************************)

(* The maintenance clock advances through the same replicated ticks as    *)
(* the session clock; a plain advance is enough here.                     *)
AdvanceClock ==
    /\ clock < MaxClock
    /\ clock' = clock + 1
    /\ UNCHANGED <<node, next_czxid, scan, victim, lastScan>>

(* A create (or delete-then-recreate) of the path. Every incarnation     *)
(* receives a fresh, strictly larger czxid.                               *)
Create(k) ==
    /\ Absent
    /\ k \in Kinds
    /\ next_czxid <= MaxCzxid
    /\ node' = <<"node", next_czxid, k, 0, clock + 1>>
    /\ next_czxid' = next_czxid + 1
    /\ UNCHANGED <<clock, scan, victim, lastScan>>

RemovePlain ==
    (* A client delete of the path. *)
    /\ ~Absent
    /\ node' = <<"absent">>
    /\ UNCHANGED <<clock, next_czxid, scan, victim, lastScan>>

AddChild ==
    /\ ~Absent
    /\ HasChildren = 0
    /\ node' = <<"node", Czxid, Kind, 1, ExpiresAt>>
    /\ UNCHANGED <<clock, next_czxid, scan, victim, lastScan>>

RemoveChild ==
    /\ ~Absent
    /\ HasChildren = 1
    /\ node' = <<"node", Czxid, Kind, 0, ExpiresAt>>
    /\ UNCHANGED <<clock, next_czxid, scan, victim, lastScan>>

(* The reaper scans expired, empty extended nodes and records the         *)
(* incarnation (czxid, kind) it observed.                                 *)
ScanCleanup ==
    /\ ~Absent
    /\ HasChildren = 0
    /\ ExpiresAt <= clock
    /\ scan[1] = "noscan"
    /\ scan' = <<"scan", Czxid, Kind>>
    /\ UNCHANGED <<clock, node, next_czxid, victim, lastScan>>

(* delete_extended: the compare-and-set. Removes only the exact          *)
(* incarnation that was scanned; anything else (including a recreated    *)
(* node) leaves the tree untouched.                                       *)
ApplyCleanup ==
    /\ scan[1] # "noscan"
    /\ IF ~Absent /\ Czxid = ScanCzxid /\ Kind = ScanKind /\ HasChildren = 0
       THEN /\ node' = <<"absent">>
            /\ victim' = <<"removed", Czxid, Kind, HasChildren>>
            /\ lastScan' = scan
       ELSE /\ UNCHANGED <<node, victim, lastScan>>
    /\ scan' = <<"noscan">>
    /\ UNCHANGED <<clock, next_czxid>>

Init ==
    /\ clock = 0
    /\ node = <<"absent">>
    /\ next_czxid = 1
    /\ scan = <<"noscan">>
    /\ victim = <<"none">>
    /\ lastScan = <<"none">>

(* Bounded models reach terminal states; explicit stuttering avoids      *)
(* spurious deadlock reports.                                             *)
DoNothing ==
    TRUE
    /\ UNCHANGED <<clock, node, next_czxid, scan, victim, lastScan>>

Next ==
    \/ AdvanceClock
    \/ \E k \in Kinds : Create(k)
    \/ RemovePlain
    \/ AddChild
    \/ RemoveChild
    \/ ScanCleanup
    \/ ApplyCleanup
    \/ DoNothing

(*************************************************************************)
(* Safety properties                                                     *)
(*************************************************************************)

CleanupRemovesOnlyScannedIncarnation ==
    (* Every applied cleanup removed exactly the incarnation it scanned:  *)
    (* a stale cleanup can never delete a recreated node.                  *)
    victim[1] = "removed" => victim[2] = lastScan[2] /\ victim[3] = lastScan[3]

CleanupNeverRemovesLiveChildfulNode ==
    (* Cleanups only remove empty nodes; children never vanish with the   *)
    (* parent.                                                             *)
    victim[1] = "removed" => victim[4] = 0

CzxidStrictlyIncreases ==
    (* Every new incarnation of the path gets a strictly larger czxid,    *)
    (* which is what makes the fencing value unique.                      *)
    ~Absent => Czxid < next_czxid

vars == <<clock, node, next_czxid, scan, victim, lastScan>>

Spec == Init /\ [][Next]_vars

=============================================================================
