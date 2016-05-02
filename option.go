package zephyr

import (
	"io"

	"github.com/savaki/zap"
)

type Option func(*Handler)

func WithHandler(handler interface{}) Option {
	return func(h *Handler) {
		switch v := handler.(type) {
		case TopicNamer:
			h.namer = v
		}

		switch v := handler.(type) {
		case MessageExtractor:
			h.extractor = v
		}

		switch v := handler.(type) {
		case Publisher:
			h.publisher = v
		}

		switch v := handler.(type) {
		case TopicArnFinder:
			h.finder = v
		}
	}
}

func WithTopicNamer(v TopicNamer) Option {
	return func(h *Handler) {
		h.namer = v
	}
}

func WithTopicNameFunc(fn TopicNameFunc) Option {
	return func(h *Handler) {
		h.namer = fn
	}
}

func WithMessageExtractor(v MessageExtractor) Option {
	return func(h *Handler) {
		h.extractor = v
	}
}

func WithPublisher(v Publisher) Option {
	return func(h *Handler) {
		h.publisher = v
	}
}

func WithPublishFunc(fn PublishFunc) Option {
	return func(h *Handler) {
		h.publisher = fn
	}
}

func WithTopicArnFinder(fn TopicArnFinder) Option {
	return func(h *Handler) {
		h.finder = fn
	}
}

func WithFindTopicArnFunc(fn FindTopicArnFunc) Option {
	return func(h *Handler) {
		h.finder = fn
	}
}

func Output(w io.Writer) Option {
	return func(h *Handler) {
		h.writer = zap.AddSync(w)
	}
}
