//! Pure state machine for Kubernetes resource lifecycle

/// The state of the generator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Creating resources (either initial or recreation)
    Initializing {
        /// If Some, we're creating a specific instance. If None, create all instances
        index: Option<u32>,
    },
    /// Running state, waiting for recreation trigger
    Running {
        /// Current index in round-robin recreation
        index: u32,
    },
    /// Deleting a specific instance before recreation
    DeletingInstance {
        /// Index of instance being deleted
        index: u32,
    },
    /// Shutting down, cleaning up resources
    ShuttingDown,
    /// Terminal state
    Terminated,
}

/// Operations the state machine can request
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Operation {
    /// Create all instances at startup
    CreateAllInstances,
    /// Create a single instance during recreation
    CreateInstance { index: u32 },
    /// Delete a single instance before recreation
    DeleteInstance { index: u32 },
    /// Wait for next action (throttle or idle)
    Wait,
    /// Delete all instances during shutdown
    DeleteAllInstances,
    /// Exit the generator
    Exit,
}

/// Events that can drive the state machine
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    /// Initial startup event, must be the first event into `StateMachine`. Must
    /// never be received again.
    Started,
    /// Creation of all instances attempted, success denoted by boolean.
    AllInstancesCreated { success: bool },
    /// Creation of a single instance with given `index`, success denoted by
    /// boolean.
    InstanceCreated { index: u32, success: bool },
    /// Deletion  of a single instance with given `index`, success denoted by
    /// boolean.
    InstanceDeleted { index: u32, success: bool },
    /// Caller is ready to create the next eligable instance.
    RecreateNext,
    /// Shutdown signal received.
    ShutdownSignaled,
    /// Attempt deletion of all instances, success denoted by boolean.
    AllInstancesDeleted { success: bool },
}

#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// Transition is not valid
    #[error("Invalid transition from {from:?} via {via:?}")]
    InvalidTransition { from: State, via: Event },
}

/// State machine for Kubernetes generator
///
/// The goal of this component is to contain the logic for state transitions
/// within this generator _without_ IO encumbrance, neither timing information
/// nor API interactions. This leaves `super::Kubernetes` to deal with the clock
/// and the k8s API. That is, that mechanism should follow the output of this
/// mechanism's `next` without consideration.
#[derive(Debug, Clone)]
pub(super) struct StateMachine {
    state: State,
    concurrent_instances: u32,
}

impl StateMachine {
    /// Create a new state machine
    pub(super) fn new(concurrent_instances: u32) -> Self {
        Self {
            state: State::Initializing { index: None },
            concurrent_instances,
        }
    }

    /// Get the current state
    pub(super) fn state(&self) -> &State {
        &self.state
    }

    /// Process an event and return the next operation
    ///
    /// # Errors
    ///
    /// Function will error with `InvalidTransition` if the `event` is not valid
    /// for the present state.
    pub(super) fn next(&mut self, event: Event) -> Result<Operation, Error> {
        let (next_state, operation) = match self.state {
            // Initial startup state: no instances exist yet, waiting to create
            // all instances
            State::Initializing { index: None } => match event {
                Event::Started => (self.state, Operation::CreateAllInstances),
                Event::AllInstancesCreated { success: true } => {
                    (State::Running { index: 0 }, Operation::Wait)
                }
                Event::AllInstancesCreated { success: false } => {
                    (State::ShuttingDown, Operation::DeleteAllInstances)
                }
                Event::ShutdownSignaled => (State::ShuttingDown, Operation::DeleteAllInstances),
                _ => {
                    return Err(Error::InvalidTransition {
                        from: self.state,
                        via: event,
                    });
                }
            },
            // Recreation state: a specific instance is being recreated after
            // deletion
            State::Initializing { index: Some(idx) } => match event {
                Event::InstanceCreated { index, success: _ } if index == idx => {
                    let next_index = (index + 1) % self.concurrent_instances;
                    (State::Running { index: next_index }, Operation::Wait)
                }
                Event::ShutdownSignaled => (State::ShuttingDown, Operation::DeleteAllInstances),
                _ => {
                    return Err(Error::InvalidTransition {
                        from: self.state,
                        via: event,
                    });
                }
            },
            // Normal operation: all instances exist, waiting to recreate the
            // next one
            State::Running { index } => match event {
                Event::RecreateNext => (
                    State::DeletingInstance { index },
                    Operation::DeleteInstance { index },
                ),
                Event::ShutdownSignaled => (State::ShuttingDown, Operation::DeleteAllInstances),
                _ => {
                    return Err(Error::InvalidTransition {
                        from: self.state,
                        via: event,
                    });
                }
            },
            // Deletion phase of recreation: removing an instance before
            // recreating it
            State::DeletingInstance { index } => match event {
                Event::InstanceDeleted {
                    index: del_idx,
                    success: _,
                } if del_idx == index => (
                    State::Initializing { index: Some(index) },
                    Operation::CreateInstance { index },
                ),
                Event::ShutdownSignaled => (State::ShuttingDown, Operation::DeleteAllInstances),
                _ => {
                    return Err(Error::InvalidTransition {
                        from: self.state,
                        via: event,
                    });
                }
            },
            // Cleanup state: generator is shutting down, deleting all instances
            State::ShuttingDown => match event {
                Event::AllInstancesDeleted { success: _ } => (State::Terminated, Operation::Exit),
                _ => {
                    return Err(Error::InvalidTransition {
                        from: self.state,
                        via: event,
                    });
                }
            },
            // Final state: all cleanup complete, no further events accepted
            State::Terminated => {
                return Err(Error::InvalidTransition {
                    from: self.state,
                    via: event,
                });
            }
        };

        self.state = next_state;
        Ok(operation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn initial_state() {
        let machine = StateMachine::new(3);
        assert_eq!(machine.state(), &State::Initializing { index: None });
    }

    proptest! {
        #[test]
        fn invalid_transitions_rejected(instances in 1u32..10u32) {
            // For each state, verify only valid events are accepted.

            // Test Initializing { index: None }
            let mut machine = StateMachine::new(instances);
            assert!(machine.next(Event::Started).is_ok());

            let mut machine = StateMachine::new(instances);
            assert!(machine.next(Event::ShutdownSignaled).is_ok());

            let mut machine = StateMachine::new(instances);
            assert!(machine.next(Event::RecreateNext).is_err());
            assert!(machine.next(Event::InstanceCreated { index: 0, success: true }).is_err());
            assert!(machine.next(Event::InstanceDeleted { index: 0, success: true }).is_err());
            assert!(machine.next(Event::AllInstancesDeleted { success: true }).is_err());

            // Test Running state
            let mut machine = StateMachine::new(instances);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllInstancesCreated { success: true }).unwrap();

            assert!(machine.next(Event::RecreateNext).is_ok());

            let mut machine = StateMachine::new(instances);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllInstancesCreated { success: true }).unwrap();

            assert!(machine.next(Event::ShutdownSignaled).is_ok());

            let mut machine = StateMachine::new(instances);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllInstancesCreated { success: true }).unwrap();

            assert!(machine.next(Event::Started).is_err());
            assert!(machine.next(Event::AllInstancesCreated { success: true }).is_err());
            assert!(machine.next(Event::InstanceCreated { index: 0, success: true }).is_err());
            assert!(machine.next(Event::InstanceDeleted { index: 0, success: true }).is_err());
            assert!(machine.next(Event::AllInstancesDeleted { success: true }).is_err());

            // Test DeletingInstance state
            let mut machine = StateMachine::new(instances);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllInstancesCreated { success: true }).unwrap();
            machine.next(Event::RecreateNext).unwrap();

            assert!(machine.next(Event::InstanceDeleted { index: 0, success: true }).is_ok());

            let mut machine = StateMachine::new(instances);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllInstancesCreated { success: true }).unwrap();
            machine.next(Event::RecreateNext).unwrap();

            assert!(machine.next(Event::ShutdownSignaled).is_ok());

            let mut machine = StateMachine::new(instances);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllInstancesCreated { success: true }).unwrap();
            machine.next(Event::RecreateNext).unwrap();

            assert!(machine.next(Event::Started).is_err());
            assert!(machine.next(Event::RecreateNext).is_err());
            assert!(machine.next(Event::AllInstancesCreated { success: true }).is_err());
            assert!(machine.next(Event::InstanceCreated { index: 0, success: true }).is_err());
            assert!(machine.next(Event::InstanceDeleted { index: 1, success: true }).is_err()); // Wrong index
            assert!(machine.next(Event::AllInstancesDeleted { success: true }).is_err());

            // Test ShuttingDown state
            let mut machine = StateMachine::new(instances);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllInstancesCreated { success: true }).unwrap();
            machine.next(Event::ShutdownSignaled).unwrap();

            assert!(machine.next(Event::AllInstancesDeleted { success: true }).is_ok());

            let mut machine = StateMachine::new(instances);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllInstancesCreated { success: true }).unwrap();
            machine.next(Event::ShutdownSignaled).unwrap();

            assert!(machine.next(Event::Started).is_err());
            assert!(machine.next(Event::RecreateNext).is_err());
            assert!(machine.next(Event::ShutdownSignaled).is_err());
            assert!(machine.next(Event::AllInstancesCreated { success: true }).is_err());
            assert!(machine.next(Event::InstanceCreated { index: 0, success: true }).is_err());
            assert!(machine.next(Event::InstanceDeleted { index: 0, success: true }).is_err());

            // Test Terminated state rejects everything
            let mut machine = StateMachine::new(instances);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllInstancesCreated { success: true }).unwrap();
            machine.next(Event::ShutdownSignaled).unwrap();
            machine.next(Event::AllInstancesDeleted { success: true }).unwrap();

            assert!(machine.next(Event::Started).is_err());
            assert!(machine.next(Event::RecreateNext).is_err());
            assert!(machine.next(Event::ShutdownSignaled).is_err());
            assert!(machine.next(Event::AllInstancesCreated { success: true }).is_err());
            assert!(machine.next(Event::InstanceCreated { index: 0, success: true }).is_err());
            assert!(machine.next(Event::InstanceDeleted { index: 0, success: true }).is_err());
            assert!(machine.next(Event::AllInstancesDeleted { success: true }).is_err());
        }

        #[test]
        fn recreation_follows_round_robin_order(instances in 2u32..20u32) {
            let mut machine = StateMachine::new(instances);

            // Initialize to running state
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllInstancesCreated { success: true }).unwrap();

            // Perform multiple recreation cycles to test ordering
            let mut expected_index = 0u32;
            for _ in 0..(instances * 3) {
                // Verify we're targeting the expected instance
                if let State::Running { index } = machine.state() {
                    assert_eq!(*index, expected_index);
                }

                machine.next(Event::RecreateNext).unwrap();

                // Verify deletion targets correct index
                if let State::DeletingInstance { index } = machine.state() {
                    assert_eq!(*index, expected_index);
                }

                machine.next(Event::InstanceDeleted { index: expected_index, success: true }).unwrap();

                // Verify recreation targets correct index
                if let State::Initializing { index: Some(index) } = machine.state() {
                    assert_eq!(*index, expected_index);
                }

                machine.next(Event::InstanceCreated { index: expected_index, success: true }).unwrap();

                // Update expected index with wrap-around
                expected_index = (expected_index + 1) % instances;

                // Verify we're now running with next index
                if let State::Running { index } = machine.state() {
                    assert_eq!(*index, expected_index);
                }
            }
        }

        #[test]
        fn success_failure_equivalence(instances in 1u32..10u32, success: bool) {
            // Both success and failure should progress the state machine for
            // most events.

            // AllInstancesCreated with success=false goes to ShuttingDown
            // instead of Running
            let mut machine = StateMachine::new(instances);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllInstancesCreated { success }).unwrap();

            if success {
                assert_eq!(machine.state(), &State::Running { index: 0 });
            } else {
                assert_eq!(machine.state(), &State::ShuttingDown);
            }

            // InstanceDeleted success/failure both progress to Initializing
            let mut machine = StateMachine::new(instances);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllInstancesCreated { success: true }).unwrap();
            machine.next(Event::RecreateNext).unwrap();
            machine.next(Event::InstanceDeleted { index: 0, success }).unwrap();
            assert_eq!(machine.state(), &State::Initializing { index: Some(0) });

            // InstanceCreated success/failure both progress to Running
            machine.next(Event::InstanceCreated { index: 0, success }).unwrap();
            assert_eq!(machine.state(), &State::Running { index: 1 % instances });

            // AllInstancesDeleted success/failure both progress to Terminated
            let mut machine = StateMachine::new(instances);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllInstancesCreated { success: true }).unwrap();
            machine.next(Event::ShutdownSignaled).unwrap();
            machine.next(Event::AllInstancesDeleted { success }).unwrap();
            assert_eq!(machine.state(), &State::Terminated);
        }

        #[test]
        fn indices_never_exceed_bounds_and_operations_match(instances in 1u32..100u32) {
            let mut machine = StateMachine::new(instances);

            // Initialize - verify operation contains valid indices
            let op = machine.next(Event::Started).unwrap();
            assert_eq!(op, Operation::CreateAllInstances);

            let op = machine.next(Event::AllInstancesCreated { success: true }).unwrap();
            assert_eq!(op, Operation::Wait);

            // Perform many recreation cycles
            for _ in 0..(instances * 3) {
                // Verify state indices are within bounds
                if let State::Running { index } = machine.state() {
                    assert!(*index < instances);
                }

                let op = machine.next(Event::RecreateNext).unwrap();

                // Verify operation indices match state and are within bounds
                let (state_idx, _op_idx) = if let (State::DeletingInstance { index: state_idx }, Operation::DeleteInstance { index: op_idx }) = (machine.state(), &op) {
                    assert_eq!(*state_idx, *op_idx);
                    assert!(*state_idx < instances);
                    assert!(*op_idx < instances);
                    (*state_idx, *op_idx)
                } else {
                    panic!("Expected DeletingInstance state and DeleteInstance operation");
                };

                let op = machine.next(Event::InstanceDeleted { index: state_idx, success: true }).unwrap();

                // Verify create operation index matches
                if let Operation::CreateInstance { index: create_idx } = op {
                    assert_eq!(create_idx, state_idx);
                    assert!(create_idx < instances);
                }

                machine.next(Event::InstanceCreated { index: state_idx, success: true }).unwrap();
            }
        }

        #[test]
        fn shutdown_reachable_from_any_state(
            instances in 1u32..10u32,
            shutdown_at in 0usize..20usize
        ) {
            // Verify ShutdownSignaled can interrupt at any point and always leads to Terminated
            let mut machine = StateMachine::new(instances);
            let mut event_count = 0;

            // Initialize
            machine.next(Event::Started).unwrap();
            event_count += 1;

            if event_count == shutdown_at {
                machine.next(Event::ShutdownSignaled).unwrap();
                machine.next(Event::AllInstancesDeleted { success: true }).unwrap();
                assert_eq!(machine.state(), &State::Terminated);
                return Ok(());
            }

            machine.next(Event::AllInstancesCreated { success: true }).unwrap();
            event_count += 1;

            // Perform recreation cycles until shutdown
            for i in 0..10 {
                if event_count == shutdown_at {
                    machine.next(Event::ShutdownSignaled).unwrap();
                    machine.next(Event::AllInstancesDeleted { success: true }).unwrap();
                    assert_eq!(machine.state(), &State::Terminated);
                    return Ok(());
                }

                machine.next(Event::RecreateNext).unwrap();
                event_count += 1;

                if event_count == shutdown_at {
                    machine.next(Event::ShutdownSignaled).unwrap();
                    machine.next(Event::AllInstancesDeleted { success: true }).unwrap();
                    assert_eq!(machine.state(), &State::Terminated);
                    return Ok(());
                }

                let index = i % instances;
                machine.next(Event::InstanceDeleted { index, success: true }).unwrap();
                event_count += 1;

                if event_count == shutdown_at {
                    machine.next(Event::ShutdownSignaled).unwrap();
                    machine.next(Event::AllInstancesDeleted { success: true }).unwrap();
                    assert_eq!(machine.state(), &State::Terminated);
                    return Ok(());
                }

                machine.next(Event::InstanceCreated { index, success: true }).unwrap();
                event_count += 1;
            }

            // Final shutdown always works
            machine.next(Event::ShutdownSignaled).unwrap();
            machine.next(Event::AllInstancesDeleted { success: true }).unwrap();
            assert_eq!(machine.state(), &State::Terminated);
        }

        #[test]
        fn state_transitions_follow_valid_paths(instances in 1u32..10u32) {
            // Verify all state transitions follow the documented state diagram
            let mut machine = StateMachine::new(instances);

            // Path 1: Normal startup -> running -> recreation cycle
            assert_eq!(machine.state(), &State::Initializing { index: None });

            let op = machine.next(Event::Started).unwrap();
            assert_eq!(op, Operation::CreateAllInstances);
            assert_eq!(machine.state(), &State::Initializing { index: None });

            let op = machine.next(Event::AllInstancesCreated { success: true }).unwrap();
            assert_eq!(op, Operation::Wait);
            assert_eq!(machine.state(), &State::Running { index: 0 });

            let op = machine.next(Event::RecreateNext).unwrap();
            assert_eq!(op, Operation::DeleteInstance { index: 0 });
            assert_eq!(machine.state(), &State::DeletingInstance { index: 0 });

            let op = machine.next(Event::InstanceDeleted { index: 0, success: true }).unwrap();
            assert_eq!(op, Operation::CreateInstance { index: 0 });
            assert_eq!(machine.state(), &State::Initializing { index: Some(0) });

            let op = machine.next(Event::InstanceCreated { index: 0, success: true }).unwrap();
            assert_eq!(op, Operation::Wait);
            assert_eq!(machine.state(), &State::Running { index: 1 % instances });

            // Path 2: Startup failure -> shutdown
            let mut machine = StateMachine::new(instances);
            machine.next(Event::Started).unwrap();

            let op = machine.next(Event::AllInstancesCreated { success: false }).unwrap();
            assert_eq!(op, Operation::DeleteAllInstances);
            assert_eq!(machine.state(), &State::ShuttingDown);

            let op = machine.next(Event::AllInstancesDeleted { success: true }).unwrap();
            assert_eq!(op, Operation::Exit);
            assert_eq!(machine.state(), &State::Terminated);

            // Path 3: Shutdown from various states
            let mut machine = StateMachine::new(instances);
            let op = machine.next(Event::ShutdownSignaled).unwrap();
            assert_eq!(op, Operation::DeleteAllInstances);
            assert_eq!(machine.state(), &State::ShuttingDown);
        }

        #[test]
        fn state_machine_deterministic(
            instances in 1u32..10u32,
            seed: u64
        ) {
            let events = generate_event_sequence(seed, instances);

            // Run the same sequence twice
            let mut machine1 = StateMachine::new(instances);
            let mut machine2 = StateMachine::new(instances);

            for event in &events {
                let result1 = machine1.next(*event);
                let result2 = machine2.next(*event);

                assert_eq!(result1, result2);
                assert_eq!(machine1.state(), machine2.state());
            }
        }
    }

    fn generate_event_sequence(seed: u64, instances: u32) -> Vec<Event> {
        use rand::{Rng, SeedableRng, rngs::StdRng};

        let mut rng = StdRng::seed_from_u64(seed);
        let mut events = Vec::new();
        let _current_state = State::Initializing { index: None };

        events.push(Event::Started);

        let success = rng.random_bool(0.5);
        events.push(Event::AllInstancesCreated { success });

        if !success {
            events.push(Event::AllInstancesDeleted { success: true });
            return events;
        }

        let _current_state = State::Running { index: 0 };

        // Generate some recreation cycles, maybe shutting down early.
        let cycles = rng.random_range(0..10);
        for i in 0..cycles {
            let index = i % instances;

            if rng.random_bool(0.1) {
                events.push(Event::ShutdownSignaled);
                events.push(Event::AllInstancesDeleted { success: true });
                return events;
            }

            events.push(Event::RecreateNext);
            events.push(Event::InstanceDeleted {
                index,
                success: rng.random_bool(0.9),
            });
            events.push(Event::InstanceCreated {
                index,
                success: rng.random_bool(0.9),
            });
        }

        events.push(Event::ShutdownSignaled);
        events.push(Event::AllInstancesDeleted { success: true });

        events
    }
}
