pub trait StateMachine {
    type Command ;
    type Response ;
    type Error ;

    fn apply(&mut self, command: Self::Command) -> Result<Self::Response, Self::Error>;
    fn snapshot(&self) -> Vec<u8>;
    fn restore(&mut self, snapshot: &[u8]) -> Result<(), Self::Error>;
}