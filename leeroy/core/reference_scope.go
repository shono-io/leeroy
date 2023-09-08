package core

import (
	"fmt"
	"strings"
)

func ParseScopeReference(s string) (ScopeReference, error) {
	if !strings.HasPrefix(s, "SCP#") {
		return ScopeReference{}, fmt.Errorf("invalid scope reference: %s", s)
	}

	parts := strings.Split(s, "#")
	if len(parts) != 2 {
		return ScopeReference{}, fmt.Errorf("invalid scope reference: %s", s)
	}

	return ScopeReference{
		Code: parts[1],
	}, nil
}

func NewScopeReference(code string) ScopeReference {
	return ScopeReference{
		Code: code,
	}
}

type ScopeReference struct {
	Code string
}

func (r ScopeReference) String() string {
	return fmt.Sprintf("SCP#%s", r.Code)
}

func (r ScopeReference) IsValid() bool {
	return r.Code != ""
}

func (r ScopeReference) Concept(code string) ConceptReference {
	return NewConceptReference(r.Code, code)
}
