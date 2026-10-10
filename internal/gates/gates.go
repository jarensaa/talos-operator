/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package gates evaluates the gates a user declares on objects in the cluster
// (docs/scope.md §6.4): a CEL expression over one object that must be true before a node is
// touched and while a changed node is verified. Nothing here reads the cluster; the caller hands
// in the object.
package gates

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
)

// costLimit bounds what evaluating one expression may cost, in CEL's own units. A comparison of a
// status field costs a handful; this leaves room for searching a list of conditions and none for
// an expression that would hold up a reconcile.
const costLimit = 100_000

// maxPrograms bounds the cache of compiled expressions. A cluster has at most a few gates, so
// reaching it means expressions are being generated, and starting over is cheaper than tracking
// which are still in use.
const maxPrograms = 256

var (
	environment = sync.OnceValues(func() (*cel.Env, error) {
		return cel.NewEnv(cel.Variable("object", cel.DynType))
	})

	mu       sync.Mutex
	programs = map[string]cel.Program{}
)

// compile returns the program for expression, compiling it once.
func compile(expression string) (cel.Program, error) {
	mu.Lock()
	defer mu.Unlock()

	if prg, ok := programs[expression]; ok {
		return prg, nil
	}

	env, err := environment()
	if err != nil {
		return nil, err
	}

	ast, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("the expression does not compile: %w", issues.Err())
	}

	// An expression over fields of unknown type has no type either until it is evaluated.
	if out := ast.OutputType(); !out.IsExactType(cel.BoolType) && !out.IsExactType(cel.DynType) {
		return nil, fmt.Errorf("the expression is a %s, not a boolean", out)
	}

	prg, err := env.Program(ast, cel.CostLimit(costLimit), cel.InterruptCheckFrequency(100))
	if err != nil {
		return nil, fmt.Errorf("the expression cannot be evaluated: %w", err)
	}

	if len(programs) >= maxPrograms {
		clear(programs)
	}

	programs[expression] = prg

	return prg, nil
}

// Check compiles expression and reports why it can never be evaluated, without an object.
func Check(expression string) error {
	_, err := compile(expression)

	return err
}

// Evaluate reports whether expression is true of object. Anything else is an error: an
// expression that does not compile, a field it reads that the object does not have, a result
// that is not a boolean, an evaluation that costs too much. The caller treats every error as a
// closed gate.
func Evaluate(ctx context.Context, expression string, object map[string]any) (bool, error) {
	if object == nil {
		return false, errors.New("there is no object to evaluate the expression on")
	}

	prg, err := compile(expression)
	if err != nil {
		return false, err
	}

	out, _, err := prg.ContextEval(ctx, map[string]any{"object": object})
	if err != nil {
		return false, fmt.Errorf("the expression could not be evaluated: %w", err)
	}

	result, ok := out.(types.Bool)
	if !ok {
		return false, fmt.Errorf("the expression evaluated to a %s, not a boolean", out.Type().TypeName())
	}

	return bool(result), nil
}
