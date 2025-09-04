//! Pure state machine for container lifecycle management

/// The state of the generator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Initial state, creating containers
    Initializing {
        /// If Some, we're creating a specific container. If None, create all containers
        index: Option<u32>,
    },
    /// Running state, waiting for recycle trigger
    Running {
        /// Current index in round-robin recycling
        index: u32,
    },
    /// Stopping a specific container before recycling
    StoppingContainer {
        /// Index of container being stopped
        index: u32,
    },
    /// Shutting down, cleaning up containers
    ShuttingDown,
    /// Terminal state
    Terminated,
}

/// Operations the state machine can request
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Operation {
    /// Create all containers at startup
    CreateAllContainers,
    /// Create a single container during recycling
    CreateContainer { index: u32 },
    /// Stop and remove a single container before recycling
    StopContainer { index: u32 },
    /// Wait for next action (throttle or idle)
    Wait,
    /// Stop and remove all containers during shutdown
    StopAllContainers,
    /// Exit the generator
    Exit,
}

/// Events that can drive the state machine
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    /// Initial startup event
    Started,
    /// Creation of all containers attempted
    AllContainersCreated { success: bool },
    /// Creation of a single container with given index
    ContainerCreated { index: u32, success: bool },
    /// Stopping of a single container with given index
    ContainerStopped { index: u32, success: bool },
    /// Caller is ready to recycle the next eligible container
    RecycleNext,
    /// Shutdown signal received
    ShutdownSignaled,
    /// Attempt to stop all containers
    AllContainersStopped { success: bool },
}

#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// Transition is not valid
    #[error("Invalid transition from {from:?} via {via:?}")]
    InvalidTransition { from: State, via: Event },
}

/// State machine for container generator
///
/// This component contains the logic for state transitions without IO encumbrance.
/// The parent `Container` struct handles Docker API interactions and timing.
#[derive(Debug, Clone)]
pub(super) struct StateMachine {
    state: State,
    concurrent_containers: u32,
}

impl StateMachine {
    /// Create a new state machine
    pub(super) fn new(concurrent_containers: u32) -> Self {
        Self {
            state: State::Initializing { index: None },
            concurrent_containers,
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
    /// Returns `InvalidTransition` if the event is not valid for the present state.
    pub(super) fn next(&mut self, event: Event) -> Result<Operation, Error> {
        let (next_state, operation) = match self.state {
            // Initial startup state: no containers exist yet
            State::Initializing { index: None } => match event {
                Event::Started => (self.state, Operation::CreateAllContainers),
                Event::AllContainersCreated { success: true } => {
                    (State::Running { index: 0 }, Operation::Wait)
                }
                Event::AllContainersCreated { success: false } => {
                    (State::ShuttingDown, Operation::StopAllContainers)
                }
                Event::ShutdownSignaled => (State::ShuttingDown, Operation::StopAllContainers),
                _ => {
                    return Err(Error::InvalidTransition {
                        from: self.state,
                        via: event,
                    });
                }
            },
            // Recreation state: a specific container is being recreated
            State::Initializing { index: Some(idx) } => match event {
                Event::ContainerCreated { index, success: _ } if index == idx => {
                    let next_index = (index + 1) % self.concurrent_containers;
                    (State::Running { index: next_index }, Operation::Wait)
                }
                Event::ShutdownSignaled => (State::ShuttingDown, Operation::StopAllContainers),
                _ => {
                    return Err(Error::InvalidTransition {
                        from: self.state,
                        via: event,
                    });
                }
            },
            // Normal operation: all containers exist, waiting to recycle
            State::Running { index } => match event {
                Event::RecycleNext => (
                    State::StoppingContainer { index },
                    Operation::StopContainer { index },
                ),
                Event::ShutdownSignaled => (State::ShuttingDown, Operation::StopAllContainers),
                _ => {
                    return Err(Error::InvalidTransition {
                        from: self.state,
                        via: event,
                    });
                }
            },
            // Stopping container before recycling
            State::StoppingContainer { index } => match event {
                Event::ContainerStopped {
                    index: stopped_idx, ..
                } if stopped_idx == index => (
                    State::Initializing { index: Some(index) },
                    Operation::CreateContainer { index },
                ),
                Event::ShutdownSignaled => (State::ShuttingDown, Operation::StopAllContainers),
                _ => {
                    return Err(Error::InvalidTransition {
                        from: self.state,
                        via: event,
                    });
                }
            },
            // Shutting down
            State::ShuttingDown => match event {
                Event::AllContainersStopped { .. } => (State::Terminated, Operation::Exit),
                _ => {
                    return Err(Error::InvalidTransition {
                        from: self.state,
                        via: event,
                    });
                }
            },
            // Terminal state
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
        fn invalid_transitions_rejected(containers in 1u32..10u32) {
            // Test Initializing { index: None }
            let mut machine = StateMachine::new(containers);
            assert!(machine.next(Event::Started).is_ok());

            let mut machine = StateMachine::new(containers);
            assert!(machine.next(Event::ShutdownSignaled).is_ok());

            let mut machine = StateMachine::new(containers);
            assert!(machine.next(Event::RecycleNext).is_err());
            assert!(machine.next(Event::ContainerCreated { index: 0, success: true }).is_err());
            assert!(machine.next(Event::ContainerStopped { index: 0, success: true }).is_err());
            assert!(machine.next(Event::AllContainersStopped { success: true }).is_err());

            // Test Running state
            let mut machine = StateMachine::new(containers);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllContainersCreated { success: true }).unwrap();

            assert!(machine.next(Event::RecycleNext).is_ok());

            let mut machine = StateMachine::new(containers);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllContainersCreated { success: true }).unwrap();

            assert!(machine.next(Event::ShutdownSignaled).is_ok());

            let mut machine = StateMachine::new(containers);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllContainersCreated { success: true }).unwrap();

            assert!(machine.next(Event::Started).is_err());
            assert!(machine.next(Event::AllContainersCreated { success: true }).is_err());
            assert!(machine.next(Event::ContainerCreated { index: 0, success: true }).is_err());
            assert!(machine.next(Event::ContainerStopped { index: 0, success: true }).is_err());
            assert!(machine.next(Event::AllContainersStopped { success: true }).is_err());

            // Test StoppingContainer state
            let mut machine = StateMachine::new(containers);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllContainersCreated { success: true }).unwrap();
            machine.next(Event::RecycleNext).unwrap();

            assert!(machine.next(Event::ContainerStopped { index: 0, success: true }).is_ok());

            let mut machine = StateMachine::new(containers);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllContainersCreated { success: true }).unwrap();
            machine.next(Event::RecycleNext).unwrap();

            assert!(machine.next(Event::ShutdownSignaled).is_ok());

            let mut machine = StateMachine::new(containers);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllContainersCreated { success: true }).unwrap();
            machine.next(Event::RecycleNext).unwrap();

            assert!(machine.next(Event::Started).is_err());
            assert!(machine.next(Event::RecycleNext).is_err());
            assert!(machine.next(Event::AllContainersCreated { success: true }).is_err());
            assert!(machine.next(Event::ContainerCreated { index: 0, success: true }).is_err());
            assert!(machine.next(Event::ContainerStopped { index: 1, success: true }).is_err()); // Wrong index
            assert!(machine.next(Event::AllContainersStopped { success: true }).is_err());

            // Test ShuttingDown state
            let mut machine = StateMachine::new(containers);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllContainersCreated { success: true }).unwrap();
            machine.next(Event::ShutdownSignaled).unwrap();

            assert!(machine.next(Event::AllContainersStopped { success: true }).is_ok());

            let mut machine = StateMachine::new(containers);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllContainersCreated { success: true }).unwrap();
            machine.next(Event::ShutdownSignaled).unwrap();

            assert!(machine.next(Event::Started).is_err());
            assert!(machine.next(Event::RecycleNext).is_err());
            assert!(machine.next(Event::ShutdownSignaled).is_err());
            assert!(machine.next(Event::AllContainersCreated { success: true }).is_err());
            assert!(machine.next(Event::ContainerCreated { index: 0, success: true }).is_err());
            assert!(machine.next(Event::ContainerStopped { index: 0, success: true }).is_err());

            // Test Terminated state rejects everything
            let mut machine = StateMachine::new(containers);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllContainersCreated { success: true }).unwrap();
            machine.next(Event::ShutdownSignaled).unwrap();
            machine.next(Event::AllContainersStopped { success: true }).unwrap();

            assert!(machine.next(Event::Started).is_err());
            assert!(machine.next(Event::RecycleNext).is_err());
            assert!(machine.next(Event::ShutdownSignaled).is_err());
            assert!(machine.next(Event::AllContainersCreated { success: true }).is_err());
            assert!(machine.next(Event::ContainerCreated { index: 0, success: true }).is_err());
            assert!(machine.next(Event::ContainerStopped { index: 0, success: true }).is_err());
            assert!(machine.next(Event::AllContainersStopped { success: true }).is_err());
        }

        #[test]
        fn recycling_follows_round_robin_order(containers in 2u32..20u32) {
            let mut machine = StateMachine::new(containers);

            // Initialize to running state
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllContainersCreated { success: true }).unwrap();

            // Perform multiple recycle cycles to test ordering
            let mut expected_index = 0u32;
            for _ in 0..(containers * 3) {
                // Verify we're targeting the expected container
                if let State::Running { index } = machine.state() {
                    assert_eq!(*index, expected_index);
                }

                machine.next(Event::RecycleNext).unwrap();

                // Verify stopping targets correct index
                if let State::StoppingContainer { index } = machine.state() {
                    assert_eq!(*index, expected_index);
                }

                machine.next(Event::ContainerStopped { index: expected_index, success: true }).unwrap();

                // Verify recreation targets correct index
                if let State::Initializing { index: Some(index) } = machine.state() {
                    assert_eq!(*index, expected_index);
                }

                machine.next(Event::ContainerCreated { index: expected_index, success: true }).unwrap();

                // Update expected index with wrap-around
                expected_index = (expected_index + 1) % containers;

                // Verify we're now running with next index
                if let State::Running { index } = machine.state() {
                    assert_eq!(*index, expected_index);
                }
            }
        }

        #[test]
        fn success_failure_equivalence(containers in 1u32..10u32, success: bool) {
            // AllContainersCreated with success=false goes to ShuttingDown
            let mut machine = StateMachine::new(containers);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllContainersCreated { success }).unwrap();

            if success {
                assert_eq!(machine.state(), &State::Running { index: 0 });
            } else {
                assert_eq!(machine.state(), &State::ShuttingDown);
            }

            // ContainerStopped success/failure both progress to Initializing
            let mut machine = StateMachine::new(containers);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllContainersCreated { success: true }).unwrap();
            machine.next(Event::RecycleNext).unwrap();
            machine.next(Event::ContainerStopped { index: 0, success }).unwrap();
            assert_eq!(machine.state(), &State::Initializing { index: Some(0) });

            // ContainerCreated success/failure both progress to Running
            machine.next(Event::ContainerCreated { index: 0, success }).unwrap();
            assert_eq!(machine.state(), &State::Running { index: 1 % containers });

            // AllContainersStopped success/failure both progress to Terminated
            let mut machine = StateMachine::new(containers);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllContainersCreated { success: true }).unwrap();
            machine.next(Event::ShutdownSignaled).unwrap();
            machine.next(Event::AllContainersStopped { success }).unwrap();
            assert_eq!(machine.state(), &State::Terminated);
        }

        #[test]
        fn indices_never_exceed_bounds(containers in 1u32..100u32) {
            let mut machine = StateMachine::new(containers);

            // Initialize
            let op = machine.next(Event::Started).unwrap();
            assert_eq!(op, Operation::CreateAllContainers);

            let op = machine.next(Event::AllContainersCreated { success: true }).unwrap();
            assert_eq!(op, Operation::Wait);

            // Perform many recycle cycles
            for _ in 0..(containers * 3) {
                // Verify state indices are within bounds
                if let State::Running { index } = machine.state() {
                    assert!(*index < containers);
                }

                let op = machine.next(Event::RecycleNext).unwrap();

                // Verify operation indices match state and are within bounds
                let (state_idx, _op_idx) = if let (State::StoppingContainer { index: state_idx }, Operation::StopContainer { index: op_idx }) = (machine.state(), &op) {
                    assert_eq!(*state_idx, *op_idx);
                    assert!(*state_idx < containers);
                    assert!(*op_idx < containers);
                    (*state_idx, *op_idx)
                } else {
                    panic!("Expected StoppingContainer state and StopContainer operation");
                };

                let op = machine.next(Event::ContainerStopped { index: state_idx, success: true }).unwrap();

                // Verify create operation index matches
                if let Operation::CreateContainer { index: create_idx } = op {
                    assert_eq!(create_idx, state_idx);
                    assert!(create_idx < containers);
                }

                machine.next(Event::ContainerCreated { index: state_idx, success: true }).unwrap();
            }
        }

        #[test]
        fn shutdown_reachable_from_any_state(
            containers in 1u32..10u32,
            shutdown_at in 0usize..20usize
        ) {
            // Verify ShutdownSignaled can interrupt at any point
            let mut machine = StateMachine::new(containers);
            let mut event_count = 0;

            // Initialize
            machine.next(Event::Started).unwrap();
            event_count += 1;

            if event_count == shutdown_at {
                machine.next(Event::ShutdownSignaled).unwrap();
                machine.next(Event::AllContainersStopped { success: true }).unwrap();
                assert_eq!(machine.state(), &State::Terminated);
                return Ok(());
            }

            machine.next(Event::AllContainersCreated { success: true }).unwrap();
            event_count += 1;

            // Perform recycle cycles until shutdown
            for i in 0..10 {
                if event_count == shutdown_at {
                    machine.next(Event::ShutdownSignaled).unwrap();
                    machine.next(Event::AllContainersStopped { success: true }).unwrap();
                    assert_eq!(machine.state(), &State::Terminated);
                    return Ok(());
                }

                machine.next(Event::RecycleNext).unwrap();
                event_count += 1;

                if event_count == shutdown_at {
                    machine.next(Event::ShutdownSignaled).unwrap();
                    machine.next(Event::AllContainersStopped { success: true }).unwrap();
                    assert_eq!(machine.state(), &State::Terminated);
                    return Ok(());
                }

                let index = i % containers;
                machine.next(Event::ContainerStopped { index, success: true }).unwrap();
                event_count += 1;

                if event_count == shutdown_at {
                    machine.next(Event::ShutdownSignaled).unwrap();
                    machine.next(Event::AllContainersStopped { success: true }).unwrap();
                    assert_eq!(machine.state(), &State::Terminated);
                    return Ok(());
                }

                machine.next(Event::ContainerCreated { index, success: true }).unwrap();
                event_count += 1;
            }

            // Final shutdown always works
            machine.next(Event::ShutdownSignaled).unwrap();
            machine.next(Event::AllContainersStopped { success: true }).unwrap();
            assert_eq!(machine.state(), &State::Terminated);
        }
    }
}
