package core

import (
	"fmt"
	"strings"
)

func ParseEventReference(s string) (EventReference, error) {
	if !strings.HasPrefix(s, "EVT#") {
		return EventReference{}, fmt.Errorf("invalid event reference: %s", s)
	}

	parts := strings.Split(s, "#")
	if len(parts) != 4 {
		return EventReference{}, fmt.Errorf("invalid event reference: %s", s)
	}

	return EventReference{
		Scope:   parts[1],
		Concept: parts[2],
		Code:    parts[3],
	}, nil
}

func NewEventReference(scope string, concept string, code string) EventReference {
	return EventReference{
		Scope:   scope,
		Concept: concept,
		Code:    code,
	}
}

type EventReference struct {
	Scope   string
	Concept string
	Code    string
}

func (r EventReference) String() string {
	return fmt.Sprintf("EVT#%s#%s#%s", r.Scope, r.Concept, r.Code)
}

func (r EventReference) IsValid() bool {
	return r.Scope != "" && r.Concept != "" && r.Code != ""
}

func (r EventReference) Parent() ConceptReference {
	return ConceptReference{
		Scope: r.Scope,
		Code:  r.Concept,
	}
}
