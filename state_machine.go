package pontoon

import (
	"log"
)

type StateMachine struct {
}

func (s *StateMachine) Apply(cr *CommandRequest) error {
	log.Printf("!!! APPLYING %+v", cr)
	return nil
}
