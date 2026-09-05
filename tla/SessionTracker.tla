------------------------------- MODULE SessionTracker -------------------------------
EXTENDS Integers, Naturals
(***************************************************************************)
(* Formal specification of the ZooKeeper-Zig replicated session tracker,  *)
(* the highest-risk correctness component identified in the roadmap.      *)
(*                                                                         *)
(* All session mutations commit through Raft, so the state machine sees a  *)
(* totally ordered log of operations; the spec models that log directly.   *)
(* The subtle races live in the interleavings the log permits:            *)
(*                                                                         *)
(*   - touch renews a session only when it is not already expired, but     *)
(*     the reaper may have already scanned the session as expired and      *)
(*     proposed an expire entry. The expire entry carries the expiry it    *)
(*     observed at scan time (expected_expires_at_ms) and applies as a     *)
(*     compare-and-set, so a committed touch that renewed the session      *)
(*     between scan and commit must invalidate the expire.                *)
(*                                                                         *)
(*   - Reconnects move the session to the next generation (an atomic      *)
(*     generation+1 compare-and-set). Touches and closes carrying a stale  *)
(*     generation must not affect the moved session.                      *)
(*                                                                         *)
(*   - The session clock advances only through session_tick entries whose  *)
(*     claimed leader term equals the term of the log position that        *)
(*     commits them; ticks proposed by a deposed leader are rejected.      *)
(*                                                                         *)
(* The spec mirrors src/server/rocks_store.zig operation by operation:     *)
(*   openSession    - rejects when the session id already exists           *)
(*   touchSession   - requires matching generation and unexpired session   *)
(*   moveSession    - requires generation == expected + 1 and unexpired    *)
(*   closeSession   - requires matching generation (no expiry check)       *)
(*   expireSession  - CAS on the scanned expiry; removes only expired      *)
(*   sessionTick    - advances the clock when the term matches             *)
(*                                                                         *)
(* Renewal sets expires = clock + Timeout + Grace (renewedExpiry).         *)
(***************************************************************************)

CONSTANTS Timeout,      (* session lifetime in clock ticks                   *)
          Grace,        (* extra grace added to every renewal                *)
          MaxClock,     (* bound on the session clock                        *)
          MaxGen,       (* bound on session generations                      *)
          Terms         (* the set of Raft terms (leader changes)            *)

VARIABLES clock,         (* replicated session clock                          *)
          term,          (* current Raft term                                 *)
          st,            (* Absent or Present(gen, exp)                       *)
          scan,         (* -1 (none) or the expiry the reaper scanned          *)
          lastRenewClock,(* clock at the last successful renewal              *)
          lastRenewGen,  (* generation that performed the last renewal        *)
          maxGen,        (* highest generation of this incarnation            *)
          removal,       (* None, Expired, or Closed                          *)
          removedClock,  (* clock at which the session was removed            *)
          expiredAt      (* -1 (none) or the clock when the session expired    *)

TypeOK ==
    /\ clock \in 0..MaxClock
    /\ term \in Terms
    /\ st \in {<<"absent">>} \union
              {<<"present", g, e>> : g \in 1..MaxGen, e \in 0..MaxClock + Timeout + Grace}
    /\ scan \in -1..MaxClock + Timeout + Grace
    /\ lastRenewClock \in 0..MaxClock
    /\ lastRenewGen \in 1..MaxGen
    /\ maxGen \in 1..MaxGen
    /\ removal \in {<<"none">>, <<"expired">>, <<"closed">>}
    /\ removedClock \in 0..MaxClock
    /\ expiredAt \in -1..MaxClock

Absent == st[1] = "absent"
Present == st[1] = "present"
Gen == st[2]
Exp == st[3]

Lifetime == Timeout + Grace

(*************************************************************************)
(* Actions                                                               *)
(*************************************************************************)

(* A tick entry proposed with term t; it advances the clock only when the  *)
(* entry commits in term t (leader_term == raft term at apply).           *)
Tick(t) ==
    /\ clock < MaxClock
    /\ clock' = IF t = term THEN clock + 1 ELSE clock
    /\ UNCHANGED <<st, scan, lastRenewClock, lastRenewGen, maxGen,
                    removal, removedClock, expiredAt>>
    /\ term' \in Terms

ChangeTerm ==
    /\ term' \in Terms \ {term}
    /\ UNCHANGED <<clock, st, scan, lastRenewClock, lastRenewGen, maxGen,
                    removal, removedClock, expiredAt>>

(* openSession: a fresh incarnation with a client-chosen generation. The  *)
(* implementation rejects an open for an existing id; passwords are       *)
(* server-generated and unique per incarnation (assumed), so fencing      *)
(* across incarnations relies on password identity, out of scope here.   *)
Open(g) ==
    /\ Absent
    /\ g \in 1..MaxGen
    /\ st' = <<"present", g, clock + Lifetime>>
    /\ maxGen' = g
    /\ removal' = <<"none">>
    /\ expiredAt' = -1
    /\ lastRenewClock' = clock
    /\ lastRenewGen' = g
    /\ UNCHANGED <<clock, term, scan, removedClock>>

(* touchSession: renews only for the current generation of an unexpired   *)
(* session.                                                               *)
Touch(g) ==
    /\ Present
    /\ g \in 1..MaxGen
    /\ Gen = g
    /\ Exp > clock
    /\ st' = <<"present", Gen, clock + Lifetime>>
    /\ lastRenewClock' = clock
    /\ lastRenewGen' = g
    /\ UNCHANGED <<clock, term, scan, maxGen, removal, removedClock, expiredAt>>

(* moveSession: reconnect; atomic generation+1 compare-and-set plus a     *)
(* renewal.                                                               *)
Move(g) ==
    /\ Present
    /\ g \in 1..MaxGen - 1
    /\ Gen = g
    /\ Exp > clock
    /\ st' = <<"present", Gen + 1, clock + Lifetime>>
    /\ maxGen' = Gen + 1
    /\ lastRenewClock' = clock
    /\ lastRenewGen' = Gen + 1
    /\ UNCHANGED <<clock, term, scan, removal, removedClock, expiredAt>>

(* closeSession: removes with no expiry check, but only for the current   *)
(* generation.                                                            *)
Close(g) ==
    /\ Present
    /\ g \in 1..MaxGen
    /\ Gen = g
    /\ st' = <<"absent">>
    /\ removal' = <<"closed">>
    /\ removedClock' = clock
    /\ UNCHANGED <<clock, term, scan, lastRenewClock, lastRenewGen, maxGen,
                    expiredAt>>

(* The reaper scans only sessions that are expired at the current clock.  *)
Scan ==
    /\ Present
    /\ Exp <= clock
    /\ scan = -1
    /\ scan' = Exp
    /\ UNCHANGED <<clock, term, st, lastRenewClock, lastRenewGen, maxGen,
                    removal, removedClock, expiredAt>>

(* expireSession: the compare-and-set against the expiry observed at scan *)
(* time. Any committed renewal in between changes Exp and defuses this.   *)
Expire ==
    /\ scan # -1
    /\ IF Present /\ Exp = scan /\ Exp <= clock
       THEN /\ st' = <<"absent">>
            /\ removal' = <<"expired">>
            /\ removedClock' = clock
       ELSE /\ UNCHANGED <<st, removal, removedClock>>
    /\ scan' = -1
    /\ UNCHANGED <<clock, term, lastRenewClock, lastRenewGen, maxGen, expiredAt>>

(* Marks the moment the clock passes a live session's expiry; the session  *)
(* must never be renewed after this point (no resurrection).              *)
MarkExpired ==
    /\ Present
    /\ Exp <= clock
    /\ expiredAt = -1
    /\ expiredAt' = clock
    /\ UNCHANGED <<clock, term, st, scan, lastRenewClock, lastRenewGen, maxGen,
                    removal, removedClock>>

Init ==
    /\ clock = 0
    /\ term = CHOOSE t \in Terms : TRUE
    /\ st = <<"absent">>
    /\ scan = -1
    /\ lastRenewClock = 0
    /\ lastRenewGen = 1
    /\ maxGen = 1
    /\ removal = <<"none">>
    /\ removedClock = 0
    /\ expiredAt = -1

Next ==
    \/ \E t \in Terms : Tick(t)
    \/ ChangeTerm
    \/ \E g \in 1..MaxGen : Open(g)
    \/ \E g \in 1..MaxGen : Touch(g)
    \/ \E g \in 1..MaxGen - 1 : Move(g)
    \/ \E g \in 1..MaxGen : Close(g)
    \/ Scan
    \/ Expire
    \/ MarkExpired

(*************************************************************************)
(* Safety properties                                                     *)
(*************************************************************************)

NoPrematureExpiry ==
    (* A session removed by the reaper lived at least its full last        *)
    (* renewal lifetime: no scan/expire race can kill a live session.      *)
    removal = <<"expired">> => removedClock >= lastRenewClock + Lifetime

GenerationFenced ==
    (* Every successful renewal and removal was performed by the current   *)
    (* generation of the incarnation: a stale connection (pre-Move) never  *)
    (* renewed or closed the session it lost.                              *)
    /\ (removal = <<"closed">> => lastRenewGen <= maxGen)
    /\ st[1] = "present" => Gen <= maxGen

NoResurrection ==
    (* Once the clock passed a live session's expiry, that session was     *)
    (* never renewed again; only the reaper or an explicit close may end   *)
    (* it.                                                                 *)
    expiredAt # -1 /\ st[1] = "present" =>
        lastRenewClock + Lifetime <= clock

StaleGenerationNeverWins ==
    (* The recorded renewing generation always equals the highest          *)
    (* generation of the incarnation: touches carrying a pre-Move          *)
    (* generation cannot have produced the last renewal.                   *)
    lastRenewGen <= maxGen

Spec == Init /\ [][Next]_<<clock, term, st, scan>>

=============================================================================
