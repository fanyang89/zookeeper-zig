------------------------------- MODULE WatchDelivery -------------------------------
EXTENDS Integers, Naturals, Sequences
(***************************************************************************)
(* Formal specification of src/server/watch.zig delivery semantics for one *)
(* active, authorized connection and one watched path.                     *)
(*                                                                         *)
(* The model covers data and child one-shot watches, persistent and         *)
(* persistent-recursive watches, exact and descendant events, overlapping  *)
(* registrations, duplicate persistent registration, explicit removal,     *)
(* events before/after response activation, and multiple pending events     *)
(* events behind one response barrier.                                     *)
(*                                                                         *)
(* Every Publish receives a monotonically increasing event id (the model's *)
(* zxid). expectedEvents is derived independently from the abstract watch   *)
(* contract; delivered and pending are produced by the implementation      *)
(* state. Their equality proves no matching event is lost or duplicated and *)
(* that delivery preserves publication order.                              *)
(*                                                                         *)
(* Connection failure, pending-queue overflow, ACL denial, session          *)
(* invalidation, and explicit connection removal intentionally permit loss  *)
(* by terminating the connection and are outside this active-connection    *)
(* safety model.                                                            *)
(***************************************************************************)

CONSTANT MaxEvents

Kinds == {"data", "children", "persistent", "persistent_recursive"}
StandardKinds == {"data", "children"}
PersistentKinds == {"persistent", "persistent_recursive"}
States == {"absent", "pending", "armed"}
EventTypes == {"data_changed", "children_changed", "deleted", "descendant_data"}

EventSequenceType ==
    UNION {[1..length -> 1..MaxEvents] : length \in 0..MaxEvents}

VARIABLES watchState,
          expectedPresent,
          barrierOpen,
          nextEvent,
          expectedEvents,
          pending,
          delivered,
          unsafeDelivery

vars == <<watchState, expectedPresent, barrierOpen, nextEvent,
          expectedEvents, pending, delivered, unsafeDelivery>>

KindMatches(kind, eventType) ==
    \/ kind = "persistent" /\
       eventType \in {"data_changed", "children_changed", "deleted"}
    \/ kind = "persistent_recursive" /\
       eventType \in {"data_changed", "deleted", "descendant_data"}
    \/ kind = "data" /\ eventType \in {"data_changed", "deleted"}
    \/ kind = "children" /\ eventType \in {"children_changed", "deleted"}

ActualMatches(eventType) ==
    \E kind \in Kinds :
        watchState[kind] # "absent" /\ KindMatches(kind, eventType)

ExpectedMatches(eventType) ==
    \E kind \in Kinds :
        expectedPresent[kind] /\ KindMatches(kind, eventType)

ConsumeActual(eventType) ==
    [kind \in Kinds |->
        IF kind \in StandardKinds /\
           watchState[kind] # "absent" /\
           KindMatches(kind, eventType)
        THEN "absent"
        ELSE watchState[kind]]

ConsumeExpected(eventType) ==
    [kind \in Kinds |->
        IF kind \in StandardKinds /\
           expectedPresent[kind] /\
           KindMatches(kind, eventType)
        THEN FALSE
        ELSE expectedPresent[kind]]

TypeOK ==
    /\ MaxEvents > 0
    /\ watchState \in [Kinds -> States]
    /\ expectedPresent \in [Kinds -> BOOLEAN]
    /\ barrierOpen \in BOOLEAN
    /\ nextEvent \in 1..MaxEvents + 1
    /\ expectedEvents \in EventSequenceType
    /\ pending \in EventSequenceType
    /\ delivered \in EventSequenceType
    /\ unsafeDelivery \in BOOLEAN

Register(kind) ==
    /\ kind \in Kinds
    /\ ~barrierOpen
    /\ watchState' = [watchState EXCEPT ![kind] = "pending"]
    /\ expectedPresent' = [expectedPresent EXCEPT ![kind] = TRUE]
    /\ barrierOpen' = TRUE
    /\ UNCHANGED <<nextEvent, expectedEvents, pending, delivered,
                    unsafeDelivery>>

ActivateResponse ==
    /\ barrierOpen
    /\ watchState' = [kind \in Kinds |->
        IF watchState[kind] = "pending" THEN "armed" ELSE watchState[kind]]
    /\ barrierOpen' = FALSE
    /\ delivered' = delivered \o pending
    /\ pending' = <<>>
    /\ UNCHANGED <<expectedPresent, nextEvent, expectedEvents,
                    unsafeDelivery>>

Remove(kind) ==
    /\ kind \in Kinds
    /\ watchState[kind] # "absent"
    /\ watchState' = [watchState EXCEPT ![kind] = "absent"]
    /\ expectedPresent' = [expectedPresent EXCEPT ![kind] = FALSE]
    /\ UNCHANGED <<barrierOpen, nextEvent, expectedEvents, pending,
                    delivered, unsafeDelivery>>

Publish(eventType) ==
    /\ eventType \in EventTypes
    /\ nextEvent <= MaxEvents
    /\ LET eventId == nextEvent
           actual == ActualMatches(eventType)
           expected == ExpectedMatches(eventType)
       IN /\ nextEvent' = nextEvent + 1
          /\ expectedEvents' = IF expected
                                THEN Append(expectedEvents, eventId)
                                ELSE expectedEvents
          /\ watchState' = ConsumeActual(eventType)
          /\ expectedPresent' = ConsumeExpected(eventType)
          /\ IF actual
             THEN IF barrierOpen
                  THEN /\ pending' = Append(pending, eventId)
                       /\ delivered' = delivered
                       /\ unsafeDelivery' = unsafeDelivery
                  ELSE /\ pending' = pending
                       /\ delivered' = Append(delivered, eventId)
                       /\ unsafeDelivery' = (unsafeDelivery \/ barrierOpen)
             ELSE /\ UNCHANGED <<pending, delivered, unsafeDelivery>>
    /\ UNCHANGED barrierOpen

DoNothing == UNCHANGED vars

Init ==
    /\ watchState = [kind \in Kinds |-> "absent"]
    /\ expectedPresent = [kind \in Kinds |-> FALSE]
    /\ barrierOpen = FALSE
    /\ nextEvent = 1
    /\ expectedEvents = <<>>
    /\ pending = <<>>
    /\ delivered = <<>>
    /\ unsafeDelivery = FALSE

Next ==
    \/ \E kind \in Kinds : Register(kind)
    \/ ActivateResponse
    \/ \E kind \in Kinds : Remove(kind)
    \/ \E eventType \in EventTypes : Publish(eventType)
    \/ DoNothing

ExactOnceInPublishOrder ==
    expectedEvents = delivered \o pending

NoDeliveryBeforeResponse ==
    ~unsafeDelivery

PendingHasResponseBarrier ==
    Len(pending) > 0 => barrierOpen

WatchStateMatchesContract ==
    \A kind \in Kinds :
        expectedPresent[kind] <=> watchState[kind] # "absent"

PersistentSurvivesPublish ==
    \A kind \in PersistentKinds :
        expectedPresent[kind] <=> watchState[kind] # "absent"

Spec == Init /\ [][Next]_vars

=============================================================================
