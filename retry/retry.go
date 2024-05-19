package retry

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type (
	Func         func(ctx context.Context) error
	Option       func(next Func) Func
	Retry        func(ctx context.Context, fn Func) error
	DelayFunc    func(duration time.Duration) time.Duration
	DelayOptions struct {
		Delay time.Duration
		Func  DelayFunc
		Max   time.Duration
	}
)

var Err = errors.New("retry error")

func New(options ...Option) Retry {
	return func(ctx context.Context, fn Func) error {
		for _, option := range options {
			fn = option(fn)
		}

		var errs []error

		for {
			err := fn(ctx)
			if err == nil {
				return nil
			}
			errs = append(errs, err)
			if errors.Is(err, Err) {
				return errors.Join(errs...)
			}
		}
	}
}

func MaxAttempts(n uint) Option {
	return func(fn Func) Func {
		var i uint
		return func(ctx context.Context) error {
			err := fn(ctx)
			if err == nil {
				return nil
			}
			i++
			if i >= n {
				err = fmt.Errorf("%w: %d attempts exceeded: %w", Err, n, err)

			}
			return err
		}
	}
}

func Delay(opt DelayOptions) Option {
	return func(fn Func) Func {
		delay := opt.Delay
		return func(ctx context.Context) error {
			err := fn(ctx)
			if err != nil {
				time.Sleep(delay)
				if opt.Func != nil {
					delay = opt.Func(delay)
				}
				if opt.Max != 0 {
					delay = min(delay, opt.Max)
				}
			}
			return err
		}
	}
}

func Timeout(duration time.Duration) Option {
	return func(fn Func) Func {
		start := time.Now()
		return func(ctx context.Context) error {
			if time.Since(start) > duration {
				return fmt.Errorf("%w: timeout %s", Err, duration)
			}
			return fn(ctx)
		}
	}
}

func Exponential(factor int) DelayFunc {
	return func(d time.Duration) time.Duration {
		return d * time.Duration(factor)
	}
}

var (
	DoubleDelay = Exponential(2)
)
