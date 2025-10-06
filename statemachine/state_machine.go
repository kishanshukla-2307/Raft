package statemachine

type StateMachine interface {
	Apply([]byte) error
	GetState() string
}
