package esti

import "strings"

type LakeCtl struct {
	rawCmd string
}

func NewLakeCtl() *LakeCtl {
	return &LakeCtl{
		rawCmd: Lakectl(),
	}
}

func (l *LakeCtl) Arg(arg string) *LakeCtl {
	l.rawCmd += " " + arg
	return l
}

// Flag Same as Arg, added for usage clarity.
func (l *LakeCtl) Flag(arg string) *LakeCtl {
	l.rawCmd += " " + arg
	return l
}

func (l *LakeCtl) PathArg(components ...string) *LakeCtl {
	l.rawCmd += " " + strings.Join(components, "/")
	return l
}

func (l *LakeCtl) URLArg(schemaPrefix string, components ...string) *LakeCtl {
	l.rawCmd += " " + schemaPrefix + strings.Join(components, "/")
	return l
}

func (l *LakeCtl) Get() string {
	return l.rawCmd
}
