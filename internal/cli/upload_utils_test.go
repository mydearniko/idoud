package cli

import "testing"

func TestAvgRateWindow(t *testing.T) {
	t.Run("empty window", func(t *testing.T) {
		if got := avgRateWindow(nil, 7); got != 0 {
			t.Fatalf("avgRateWindow(nil, 7) = %v, want 0", got)
		}
	})

	t.Run("zero-filled startup semantics", func(t *testing.T) {
		window := make([]float64, 0, 7)
		window = pushRate(window, 700, 7)
		window = pushRate(window, 0, 7)

		got := avgRateWindow(window, 7)
		want := 100.0 // (700 + 0 + 5*0) / 7
		if got != want {
			t.Fatalf("avgRateWindow(startup) = %v, want %v", got, want)
		}
	})

	t.Run("full window", func(t *testing.T) {
		window := []float64{700, 700, 700, 700, 700, 700, 700}
		got := avgRateWindow(window, 7)
		want := 700.0
		if got != want {
			t.Fatalf("avgRateWindow(full) = %v, want %v", got, want)
		}
	})
}
