package output

import "testing"

// Flag is the formatter behind the CONFLATION column (PLAN_CONFLATION §2.6).
// Its whole job is that a scanned column shows only the rows that have the
// policy: "false" and "the broker is too old to say" must look the same,
// because in both cases the group is not conflating.
func TestFlagRendersOnlyWhatIsSet(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   any
		want string
	}{
		{"true", true, "yes"},
		{"false", false, "-"},
		{"absent", nil, "-"},
		{"string true", "true", "yes"},
		{"string TRUE", "TRUE", "yes"},
		{"string false", "false", "-"},
		{"nonsense", 42, "-"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Flag(tc.in); got != tc.want {
				t.Errorf("Flag(%#v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
