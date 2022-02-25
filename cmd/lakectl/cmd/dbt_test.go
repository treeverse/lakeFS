package cmd

import "testing"

func TestDbtClient_Debug(t *testing.T) {
	type fields struct {
		projectDir string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := DbtClient{
				projectDir: tt.fields.projectDir,
			}
			if got := d.Debug(); got != tt.want {
				t.Errorf("Debug() = %v, want %v", got, tt.want)
			}
		})
	}
}
