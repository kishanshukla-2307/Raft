package statemachine

var (
	_ StateMachine = (*SimpleStateMachine)(nil)
)

// a state machine that just appends new cmd to its state
type SimpleStateMachine struct {
	State string
}

func NewSimpleStateMachine() *SimpleStateMachine {
	return &SimpleStateMachine{
		State: "",
	}
}

func (ssm *SimpleStateMachine) Apply(cmd []byte) error {
	ssm.State += (string)(cmd) + ","
	return nil
}

func (ssm *SimpleStateMachine) GetState() string {
	return ssm.State
}
