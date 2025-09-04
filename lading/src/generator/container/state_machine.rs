//! Pure state machine for container lifecycle management

/// The state of the generator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Initial state, need to create all containers
    Starting,
    /// Running state, all containers exist
    Running {
        /// Next container index to recycle (round-robin)
        next_recycle_index: u32,
    },
    /// Recycling a specific container
    Recycling {
        /// Index of container being recycled
        index: u32,
    },
    /// Shutting down
    ShuttingDown,
    /// Terminal state
    Terminated,
}

/// Operations the state machine can request
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Operation {
    /// Create all containers at startup
    CreateAllContainers,
    /// Replace a container (stop old, start new)
    RecycleContainer { index: u32 },
    /// Wait for next action (throttle or idle)
    Wait,
    /// Stop all containers during shutdown
    StopAllContainers,
    /// Exit the generator
    Exit,
}

/// Events that can drive the state machine
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    /// Initial startup
    Started,
    /// All containers created
    AllContainersReady,
    /// Container recycled (old stopped, new started)
    ContainerRecycled { index: u32 },
    /// Time to recycle next container
    RecycleNext,
    /// Shutdown signal received
    ShutdownSignaled,
    /// All containers stopped
    AllContainersStopped,
}

#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// Transition is not valid
    #[error("Invalid transition from {from:?} via {via:?}")]
    InvalidTransition { from: State, via: Event },
}

/// State machine for container generator
///
/// The goal of this component is to contain the logic for state transitions
/// within this generator _without_ IO encumbrance, neither timing information
/// nor Docker API interactions. This leaves `super::Container` to deal with the clock
/// and the Docker API. That is, that mechanism should follow the output of this
/// mechanism's `next` without consideration.
#[derive(Debug, Clone)]
pub(super) struct StateMachine {
    state: State,
    concurrent_containers: u32,
}

impl StateMachine {
    /// Create a new state machine
    pub(super) fn new(concurrent_containers: u32) -> Self {
        Self {
            state: State::Starting,
            concurrent_containers,
        }
    }

    /// Get the current state
    pub(super) fn state(&self) -> &State {
        &self.state
    }

    /// Process an event and return the next operation
    ///
    /// State transitions:
    /// ```text
    /// Format: CurrentState --[Event]--> NextState (Operation)
    ///
    /// Starting --[Started]--> Starting (CreateAllContainers)
    /// Starting --[AllContainersReady]--> Running (Wait)
    /// Starting --[ShutdownSignaled]--> ShuttingDown (StopAllContainers)
    ///
    /// Running --[RecycleNext]--> Recycling (RecycleContainer)
    /// Running --[ShutdownSignaled]--> ShuttingDown (StopAllContainers)
    ///
    /// Recycling --[ContainerRecycled]--> Running (Wait)
    /// Recycling --[ShutdownSignaled]--> ShuttingDown (StopAllContainers)
    ///
    /// ShuttingDown --[AllContainersStopped]--> Terminated (Exit)
    /// ```
    ///
    /// # Errors
    ///
    /// Function will error with `InvalidTransition` if the `event` is not valid
    /// for the present state.
    pub(super) fn next(&mut self, event: Event) -> Result<Operation, Error> {
        let (next_state, operation) = match (self.state, event) {
            // Starting state transitions
            (State::Starting, Event::Started) => (State::Starting, Operation::CreateAllContainers),
            (State::Starting, Event::AllContainersReady) => (
                State::Running {
                    next_recycle_index: 0,
                },
                Operation::Wait,
            ),
            // Shutdown can happen from any non-terminal state
            (
                State::Starting | State::Running { .. } | State::Recycling { .. },
                Event::ShutdownSignaled,
            ) => (State::ShuttingDown, Operation::StopAllContainers),

            // Running state transitions
            (State::Running { next_recycle_index }, Event::RecycleNext) => (
                State::Recycling {
                    index: next_recycle_index,
                },
                Operation::RecycleContainer {
                    index: next_recycle_index,
                },
            ),

            // Recycling state transitions
            (
                State::Recycling { index },
                Event::ContainerRecycled {
                    index: recycled_idx,
                },
            ) if index == recycled_idx => {
                let next_index = (index + 1) % self.concurrent_containers;
                (
                    State::Running {
                        next_recycle_index: next_index,
                    },
                    Operation::Wait,
                )
            }

            // Shutting down state transitions
            (State::ShuttingDown, Event::AllContainersStopped) => {
                (State::Terminated, Operation::Exit)
            }

            // Any other transition is invalid
            _ => {
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
        assert_eq!(machine.state(), &State::Starting);
    }

    #[test]
    fn basic_lifecycle() {
        let mut machine = StateMachine::new(3);

        // Start -> create all containers
        let op = machine.next(Event::Started).unwrap();
        assert_eq!(op, Operation::CreateAllContainers);
        assert_eq!(machine.state(), &State::Starting);

        // Containers created -> running
        let op = machine.next(Event::AllContainersReady).unwrap();
        assert_eq!(op, Operation::Wait);
        assert_eq!(
            machine.state(),
            &State::Running {
                next_recycle_index: 0
            }
        );

        // Recycle first container
        let op = machine.next(Event::RecycleNext).unwrap();
        assert_eq!(op, Operation::RecycleContainer { index: 0 });
        assert_eq!(machine.state(), &State::Recycling { index: 0 });

        let op = machine.next(Event::ContainerRecycled { index: 0 }).unwrap();
        assert_eq!(op, Operation::Wait);
        assert_eq!(
            machine.state(),
            &State::Running {
                next_recycle_index: 1
            }
        );

        // Recycle second container
        let op = machine.next(Event::RecycleNext).unwrap();
        assert_eq!(op, Operation::RecycleContainer { index: 1 });

        let op = machine.next(Event::ContainerRecycled { index: 1 }).unwrap();
        assert_eq!(op, Operation::Wait);
        assert_eq!(
            machine.state(),
            &State::Running {
                next_recycle_index: 2
            }
        );

        // Shutdown
        let op = machine.next(Event::ShutdownSignaled).unwrap();
        assert_eq!(op, Operation::StopAllContainers);
        assert_eq!(machine.state(), &State::ShuttingDown);

        let op = machine.next(Event::AllContainersStopped).unwrap();
        assert_eq!(op, Operation::Exit);
        assert_eq!(machine.state(), &State::Terminated);
    }

    proptest! {
        #[test]
        fn round_robin_recycling(containers in 2u32..10u32) {
            let mut machine = StateMachine::new(containers);

            // Initialize to running state
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllContainersReady).unwrap();

            // Perform multiple recycle cycles to test round-robin ordering
            for cycle in 0..3 {
                for idx in 0..containers {
                    let expected_idx = idx;

                    // Verify we're recycling the expected container
                    if let State::Running { next_recycle_index } = machine.state() {
                        assert_eq!(*next_recycle_index, expected_idx);
                    }

                    // Trigger recycle
                    let op = machine.next(Event::RecycleNext).unwrap();
                    assert_eq!(op, Operation::RecycleContainer { index: expected_idx });

                    // Complete recycle
                    machine.next(Event::ContainerRecycled { index: expected_idx }).unwrap();

                    // Verify next index wraps around correctly
                    let next_expected = (expected_idx + 1) % containers;
                    if let State::Running { next_recycle_index } = machine.state() {
                        assert_eq!(*next_recycle_index, next_expected,
                                   "After recycling container {expected_idx} in cycle {cycle}, expected next index {next_expected}");
                    }
                }
            }
        }

        #[test]
        fn indices_always_in_bounds(containers in 1u32..20u32) {
            let mut machine = StateMachine::new(containers);

            machine.next(Event::Started).unwrap();
            machine.next(Event::AllContainersReady).unwrap();

            // Perform many cycles to test bounds
            for _ in 0..(containers * 5) {
                // Check running state index
                if let State::Running { next_recycle_index } = machine.state() {
                    assert!(*next_recycle_index < containers,
                            "Running index {index} out of bounds for {count} containers",
                            index = next_recycle_index, count = containers);
                }

                // Trigger recycle and check operation index
                if let Operation::RecycleContainer { index } = machine.next(Event::RecycleNext).unwrap() {
                    assert!(index < containers,
                            "Recycle index {index} out of bounds for {count} containers",
                            index = index, count = containers);

                    // Complete the recycle
                    machine.next(Event::ContainerRecycled { index }).unwrap();
                }
            }
        }

        #[test]
        fn shutdown_from_any_state(containers in 1u32..5u32) {
            // Test shutdown from Starting state
            let mut machine = StateMachine::new(containers);
            machine.next(Event::ShutdownSignaled).unwrap();
            machine.next(Event::AllContainersStopped).unwrap();
            assert_eq!(machine.state(), &State::Terminated);

            // Test shutdown from Running state
            let mut machine = StateMachine::new(containers);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllContainersReady).unwrap();
            machine.next(Event::ShutdownSignaled).unwrap();
            machine.next(Event::AllContainersStopped).unwrap();
            assert_eq!(machine.state(), &State::Terminated);

            // Test shutdown from Recycling state
            let mut machine = StateMachine::new(containers);
            machine.next(Event::Started).unwrap();
            machine.next(Event::AllContainersReady).unwrap();
            machine.next(Event::RecycleNext).unwrap();
            machine.next(Event::ShutdownSignaled).unwrap();
            machine.next(Event::AllContainersStopped).unwrap();
            assert_eq!(machine.state(), &State::Terminated);
        }

        #[test]
        fn invalid_transitions_rejected(containers in 1u32..5u32) {
            let mut machine = StateMachine::new(containers);

            // Can't recycle before starting
            assert!(machine.next(Event::RecycleNext).is_err());
            assert!(machine.next(Event::ContainerRecycled { index: 0 }).is_err());

            // Start normally
            machine.next(Event::Started).unwrap();

            // Can't recycle before containers ready
            assert!(machine.next(Event::RecycleNext).is_err());
            assert!(machine.next(Event::ContainerRecycled { index: 0 }).is_err());

            // Move to running
            machine.next(Event::AllContainersReady).unwrap();

            // Can't send wrong events in running state
            assert!(machine.next(Event::Started).is_err());
            assert!(machine.next(Event::AllContainersReady).is_err());
            assert!(machine.next(Event::ContainerRecycled { index: 0 }).is_err());

            // Move to recycling
            machine.next(Event::RecycleNext).unwrap();

            // Can't send wrong events in recycling state
            assert!(machine.next(Event::Started).is_err());
            assert!(machine.next(Event::AllContainersReady).is_err());
            assert!(machine.next(Event::RecycleNext).is_err());

            // Wrong index should be rejected
            assert!(machine.next(Event::ContainerRecycled { index: 1 }).is_err());

            // Correct index works
            machine.next(Event::ContainerRecycled { index: 0 }).unwrap();

            // Move to shutdown
            machine.next(Event::ShutdownSignaled).unwrap();

            // Can't do anything except complete shutdown
            assert!(machine.next(Event::Started).is_err());
            assert!(machine.next(Event::RecycleNext).is_err());
            assert!(machine.next(Event::AllContainersReady).is_err());

            // Complete shutdown
            machine.next(Event::AllContainersStopped).unwrap();

            // Terminal state rejects everything
            assert!(machine.next(Event::Started).is_err());
            assert!(machine.next(Event::ShutdownSignaled).is_err());
            assert!(machine.next(Event::AllContainersStopped).is_err());
        }
    }
}
