package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"io"
	"log"
	"log/slog"
	"net/http"
)

type HTTPServer struct {
	addr     string
	mntPoint string
	store    *KVStore
	logger   *slog.Logger
}

func NewHTTPServer(addr, mntPoint string, store *KVStore) *HTTPServer {
	return &HTTPServer{
		addr:     addr,
		mntPoint: mntPoint,
		store:    store,
		logger:   logger,
	}
}

func (s *HTTPServer) Serve(ctx context.Context) error {
	srv := &http.Server{
		Addr:    s.addr,
		Handler: s.Routes(),
	}

	shutdownCh := make(chan struct{})
	go func(ctx context.Context) {
		<-ctx.Done()
		s.logger.Info("Shutting down kvstore http server")
		if err := srv.Shutdown(context.Background()); err != nil {
			s.logger.Error("Error shutting down kvstore http server", err)
			_ = srv.Close()
		}
		close(shutdownCh)
	}(ctx)
	s.logger.Info("Starting kvstore http server")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	<-shutdownCh
	return nil
}

func (s *HTTPServer) Routes() chi.Router {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Logger)

	r.Route(s.mntPoint, func(r chi.Router) {
		r.Get("/{key}", s.Get)
		r.Post("/", s.Post)
		r.Put("/{key}", s.Put)
	})
	return r
}

func (s *HTTPServer) Get(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	v, ok := s.store.Get(key)
	if !ok {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	_, _ = w.Write([]byte(v))
}

func (s *HTTPServer) Put(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	value, err := io.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	_, ok := s.store.Get(key)
	if !ok {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	s.store.Put(key, string(value))
}

func (s *HTTPServer) Post(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	var e map[string]string
	err = json.Unmarshal(data, &e)
	if err != nil {
		s.logger.Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
}
