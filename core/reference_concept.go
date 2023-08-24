package core

import (
	"fmt"
	"strings"
)

func ParseConceptReference(s string) (ConceptReference, error) {
	if !strings.HasPrefix(s, "CON#") {
		return ConceptReference{}, fmt.Errorf("invalid concept reference: %s", s)
	}

	parts := strings.Split(s, "#")
	if len(parts) != 3 {
		return ConceptReference{}, fmt.Errorf("invalid concept reference: %s", s)
	}

	return ConceptReference{
		Scope: parts[1],
		Code:  parts[2],
	}, nil
}

func NewConceptReference(scope string, code string) ConceptReference {
	return ConceptReference{
		Scope: scope,
		Code:  code,
	}
}

type ConceptReference struct {
	Scope string
	Code  string
}

func (r ConceptReference) String() string {
	return fmt.Sprintf("CON#%s#%s", r.Scope, r.Code)
}

func (r ConceptReference) IsValid() bool {
	return r.Scope != "" && r.Code != ""
}

func (r ConceptReference) Parent() ScopeReference {
	return ScopeReference{
		Code: r.Scope,
	}
}

func (r ConceptReference) Event(code string) EventReference {
	return NewEventReference(r.Scope, r.Code, code)
}
