package lua

import (
	"context"
	"io"
	"runtime"
	"strconv"
	"strings"

	glua "github.com/Shopify/go-lua"
)

// This file is an adaptation of https://github.com/Shopify/go-lua/blob/main/base.go
// Original MIT license with copyright (as appears in https://github.com/Shopify/go-lua/blob/main/LICENSE.md):
/*
The MIT License (MIT)

Copyright (c) 2014 Shopify Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

func next(l *glua.State) int {
	glua.CheckType(l, 1, glua.TypeTable)
	l.SetTop(2)
	if l.Next(1) {
		return 2
	}
	l.PushNil()
	return 1
}

func pairs(method string, isZero bool, iter glua.Function) glua.Function {
	return func(l *glua.State) int {
		if hasMetamethod := glua.MetaField(l, 1, method); !hasMetamethod {
			glua.CheckType(l, 1, glua.TypeTable) // argument must be a table
			l.PushGoFunction(iter)               // will return generator,
			l.PushValue(1)                       // state,
			if isZero {                          // and initial value
				l.PushInteger(0)
			} else {
				l.PushNil()
			}
		} else {
			l.PushValue(1) // argument 'self' to metamethod
			l.Call(1, 3)   // get 3 values from metamethod
		}
		return 3
	}
}

func intPairs(l *glua.State) int {
	i := glua.CheckInteger(l, 2)
	glua.CheckType(l, 1, glua.TypeTable)
	i++ // next value
	l.PushInteger(i)
	l.RawGetInt(1, i)
	if l.IsNil(-1) {
		return 1
	}
	return 2
}

func finishProtectedCall(l *glua.State, status bool) int {
	if !l.CheckStack(1) {
		l.SetTop(0) // create space for return values
		l.PushBoolean(false)
		l.PushString("stack overflow")
		return 2 // return false, message
	}
	l.PushBoolean(status) // first result (status)
	l.Replace(1)          // put first result in the first slot
	return l.Top()
}

func protectedCallContinuation(l *glua.State) int {
	_, shouldYield, _ := l.Context()
	return finishProtectedCall(l, shouldYield)
}

func loadHelper(l *glua.State, s error, e int) int {
	if s == nil {
		if e != 0 {
			l.PushValue(e)
			if _, ok := glua.SetUpValue(l, -2, 1); !ok {
				l.Pop(1)
			}
		}
		return 1
	}
	l.PushNil()
	l.Insert(-2)
	return 2
}

type genericReader struct {
	l *glua.State
	r *strings.Reader
	e error
}

func (r *genericReader) Read(b []byte) (n int, err error) {
	if r.e != nil {
		return 0, r.e
	}
	if l := r.l; r.r == nil {
		glua.CheckStackWithMessage(l, 2, "too many nested functions")
		l.PushValue(1)
		if l.Call(0, 1); l.IsNil(-1) {
			l.Pop(1)
			return 0, io.EOF
		} else if !l.IsString(-1) {
			glua.Errorf(l, "reader function must return a string")
		}
		if s, ok := l.ToString(-1); ok {
			r.r = strings.NewReader(s)
		} else {
			return 0, io.EOF
		}
	}
	if n, err = r.r.Read(b); err == io.EOF {
		r.r, err = nil, nil
	} else if err != nil {
		r.e = err
	}
	return
}

func getBaseLibrary(output io.StringWriter) []glua.RegistryFunction {
	return []glua.RegistryFunction{
		{Name: "assert", Function: func(l *glua.State) int {
			if !l.ToBoolean(1) {
				glua.Errorf(l, "%s", glua.OptString(l, 2, "assertion failed!"))
				panic("unreachable")
			}
			return l.Top()
		}},
		{Name: "collectgarbage", Function: func(l *glua.State) int {
			switch opt, _ := glua.OptString(l, 1, "collect"), glua.OptInteger(l, 2, 0); opt {
			case "collect":
				runtime.GC()
				l.PushInteger(0)
			case "step":
				runtime.GC()
				l.PushBoolean(true)
			case "count":
				var stats runtime.MemStats
				runtime.ReadMemStats(&stats)
				l.PushNumber(float64(stats.HeapAlloc >> 10))
				l.PushInteger(int(stats.HeapAlloc & 0x3ff))
				return 2
			default:
				l.PushInteger(-1)
			}
			return 1
		}},
		// {"dofile", func(l *glua.State) int {
		// 	f := glua.OptString(l, 1, "")
		// 	if l.SetTop(1); glua.LoadFile(l, f, "") != nil {
		// 		l.Error()
		// 		panic("unreachable")
		// 	}
		// 	continuation := func(l *glua.State) int { return l.Top() - 1 }
		// 	l.CallWithContinuation(0, glua.MultipleReturns, 0, continuation)
		// 	return continuation(l)
		// }},
		{Name: "error", Function: func(l *glua.State) int {
			level := glua.OptInteger(l, 2, 1)
			l.SetTop(1)
			if l.IsString(1) && level > 0 {
				glua.Where(l, level)
				l.PushValue(1)
				l.Concat(2)
			}
			l.Error()
			panic("unreachable")
		}},
		{Name: "getmetatable", Function: func(l *glua.State) int {
			glua.CheckAny(l, 1)
			if !l.MetaTable(1) {
				l.PushNil()
				return 1
			}
			glua.MetaField(l, 1, "__metatable")
			return 1
		}},
		{Name: "ipairs", Function: pairs("__ipairs", true, intPairs)},
		// {"loadfile", func(l *glua.State) int {
		// 	f, m, e := glua.OptString(l, 1, ""), glua.OptString(l, 2, ""), 3
		// 	if l.IsNone(e) {
		// 		e = 0
		// 	}
		// 	return loadHelper(l, glua.LoadFile(l, f, m), e)
		// }},
		{Name: "load", Function: func(l *glua.State) int {
			m, e := glua.OptString(l, 3, "bt"), 4
			if l.IsNone(e) {
				e = 0
			}
			var err error
			if s, ok := l.ToString(1); ok {
				err = glua.LoadBuffer(l, s, glua.OptString(l, 2, s), m)
			} else {
				chunkName := glua.OptString(l, 2, "=(load)")
				glua.CheckType(l, 1, glua.TypeFunction)
				err = l.Load(&genericReader{l: l}, chunkName, m)
			}
			return loadHelper(l, err, e)
		}},
		{Name: "next", Function: next},
		{Name: "pairs", Function: pairs("__pairs", false, next)},
		{Name: "pcall", Function: func(l *glua.State) int {
			glua.CheckAny(l, 1)
			l.PushNil()
			l.Insert(1) // create space for status result
			return finishProtectedCall(l, nil == l.ProtectedCallWithContinuation(l.Top()-2, glua.MultipleReturns, 0, 0, protectedCallContinuation))
		}},
		{Name: "print", Function: func(l *glua.State) int {
			n := l.Top()
			l.Global("tostring")
			for i := 1; i <= n; i++ {
				l.PushValue(-1) // function to be called
				l.PushValue(i)  // value to print
				l.Call(1, 1)
				s, ok := l.ToString(-1)
				if !ok {
					glua.Errorf(l, "'tostring' must return a string to 'print'")
					panic("unreachable")
				}
				if i > 1 {
					_, _ = output.WriteString("\t")
				}
				_, _ = output.WriteString(s)
				l.Pop(1) // pop result
			}
			_, _ = output.WriteString("\n")
			return 0
		}},
		{Name: "rawequal", Function: func(l *glua.State) int {
			glua.CheckAny(l, 1)
			glua.CheckAny(l, 2)
			l.PushBoolean(l.RawEqual(1, 2))
			return 1
		}},
		{Name: "rawlen", Function: func(l *glua.State) int {
			t := l.TypeOf(1)
			glua.ArgumentCheck(l, t == glua.TypeTable || t == glua.TypeString, 1, "table or string expected")
			l.PushInteger(l.RawLength(1))
			return 1
		}},
		{Name: "rawget", Function: func(l *glua.State) int {
			glua.CheckType(l, 1, glua.TypeTable)
			glua.CheckAny(l, 2)
			l.SetTop(2)
			l.RawGet(1)
			return 1
		}},
		{Name: "rawset", Function: func(l *glua.State) int {
			glua.CheckType(l, 1, glua.TypeTable)
			glua.CheckAny(l, 2)
			glua.CheckAny(l, 3)
			l.SetTop(3)
			l.RawSet(1)
			return 1
		}},
		{Name: "select", Function: func(l *glua.State) int {
			n := l.Top()
			if l.TypeOf(1) == glua.TypeString {
				if s, _ := l.ToString(1); s[0] == '#' {
					l.PushInteger(n - 1)
					return 1
				}
			}
			i := glua.CheckInteger(l, 1)
			if i < 0 {
				i = n + i
			} else if i > n {
				i = n
			}
			glua.ArgumentCheck(l, 1 <= i, 1, "index out of range")
			return n - i
		}},
		{Name: "setmetatable", Function: func(l *glua.State) int {
			t := l.TypeOf(2)
			glua.CheckType(l, 1, glua.TypeTable)
			glua.ArgumentCheck(l, t == glua.TypeNil || t == glua.TypeTable, 2, "nil or table expected")
			if glua.MetaField(l, 1, "__metatable") {
				glua.Errorf(l, "cannot change a protected metatable")
			}
			l.SetTop(2)
			l.SetMetaTable(1)
			return 1
		}},
		{Name: "tonumber", Function: func(l *glua.State) int {
			if l.IsNoneOrNil(2) { // standard conversion
				if n, ok := l.ToNumber(1); ok {
					l.PushNumber(n)
					return 1
				}
				glua.CheckAny(l, 1)
			} else {
				s := glua.CheckString(l, 1)
				base := glua.CheckInteger(l, 2)
				glua.ArgumentCheck(l, 2 <= base && base <= 36, 2, "base out of range")
				if i, err := strconv.ParseInt(strings.TrimSpace(s), base, 64); err == nil {
					l.PushNumber(float64(i))
					return 1
				}
			}
			l.PushNil()
			return 1
		}},
		{Name: "tostring", Function: func(l *glua.State) int {
			glua.CheckAny(l, 1)
			glua.ToStringMeta(l, 1)
			return 1
		}},
		{Name: "type", Function: func(l *glua.State) int {
			glua.CheckAny(l, 1)
			l.PushString(glua.TypeNameOf(l, 1))
			return 1
		}},
		{Name: "xpcall", Function: func(l *glua.State) int {
			n := l.Top()
			glua.ArgumentCheck(l, n >= 2, 2, "value expected")
			l.PushValue(1) // exchange function and error handler
			l.Copy(2, 1)
			l.Replace(2)
			return finishProtectedCall(l, nil == l.ProtectedCallWithContinuation(n-2, glua.MultipleReturns, 1, 0, protectedCallContinuation))
		}},
	}
}

// BaseOpen opens the basic library. Usually passed to Require.
func BaseOpen(buf io.StringWriter) glua.Function {
	return func(l *glua.State) int {
		l.PushGlobalTable()
		l.PushGlobalTable()
		l.SetField(-2, "_G")
		glua.SetFunctions(l, getBaseLibrary(buf), 0)
		l.PushString(glua.VersionString)
		l.SetField(-2, "_VERSION")
		return 1
	}
}

func OpenSafe(l *glua.State, ctx context.Context, buf io.StringWriter) {
	// a thin version of the standard library that doesn't include 'io'
	//  along with a set of globals that omit anything that loads something external or reaches out to OS.
	libs := []glua.RegistryFunction{
		{Name: "_G", Function: BaseOpen(buf)},
		{Name: "package", Function: glua.PackageOpen},
		{Name: "table", Function: glua.TableOpen},
		{Name: "string", Function: glua.StringOpen},
		{Name: "bit32", Function: glua.Bit32Open},
		{Name: "math", Function: glua.MathOpen},
		{Name: "debug", Function: glua.DebugOpen},
	}
	for _, lib := range libs {
		glua.Require(l, lib.Name, lib.Function, true)
		l.Pop(1)
	}

	// utils adapted from goluago, skipping anything that allows network or storage access.
	// additionally, the "goluago" namespace is removed from import tokens
	Open(l, ctx)
}
