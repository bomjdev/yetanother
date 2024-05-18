package retry

import (
	"context"
	"fmt"
	"time"
)

type (
	Func      func(ctx context.Context) error
	Retry     func(ctx context.Context, f Func) error
	DelayFunc func(d time.Duration) time.Duration
	Options   struct {
		Attempts  uint
		Timeout   time.Duration
		Delay     time.Duration
		MaxDelay  time.Duration
		DelayFunc DelayFunc
	}
)

func Factory(options Options) Retry {
	return func(ctx context.Context, f Func) error {
		var (
			attempts uint
			start    = time.Now()
			cancel   context.CancelFunc
		)

		if options.Timeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, options.Timeout)
			defer cancel()
		}

		for {
			if err := f(ctx); err == nil {
				return nil
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			attempts++
			if options.Attempts > 0 && attempts >= options.Attempts {
				return fmt.Errorf("max %d attempts reached", options.Attempts)
			}

			if options.Timeout > 0 && time.Since(start) > options.Timeout {
				return fmt.Errorf("timeout %s exceeded", options.Timeout)
			}

			time.Sleep(options.Delay)

			if options.DelayFunc != nil {
				options.Delay = options.DelayFunc(options.Delay)
			}

			if options.MaxDelay > 0 && options.Delay > options.MaxDelay {
				options.Delay = options.MaxDelay
			}
		}
	}
}

func Exponential(factor int) DelayFunc {
	return func(d time.Duration) time.Duration {
		return d * time.Duration(factor)
	}
}

var (
	Double DelayFunc = Exponential(2)
)
