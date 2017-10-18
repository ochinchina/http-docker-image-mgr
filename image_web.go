package main

import (
    "encoding/json"
    "net/http"
    "strings"
)

type ImageWeb struct {
    image_storage ImageStorage
}

func NewImageWeb( image_storage ImageStorage ) *ImageWeb {
    iw := &ImageWeb{ image_storage: image_storage }
    iw.init()
    return iw
}

func (iw *ImageWeb) init() {
    http.HandleFunc("/image/get/", func(rw http.ResponseWriter, req *http.Request) {
        a := strings.Split(req.URL.Path, "/")
        iw.image_storage.Get(a[len(a)-1], rw )

    })

    http.HandleFunc("/image/list", func(rw http.ResponseWriter, req *http.Request) {
        if images, err := iw.image_storage.List(); err == nil {
            rw.Header().Set("Content-Type", "application/json") // normal header
            if b, err := json.Marshal(images); err == nil {
                rw.Write(b)
            }
        }

    })
    http.HandleFunc("/image/save/", func(rw http.ResponseWriter, req *http.Request) {
        image_name_info := strings.Split(req.URL.Path, "/")
        n := len( image_name_info )
        if req.Method == "POST" {
            defer req.Body.Close()
            err := iw.image_storage.Write( image_name_info[n-2] + ":" + image_name_info[n-1], req.Body )
            if err == nil {
                rw.Write( []byte("save image successfully" ) )
            } else {
                rw.Write( []byte("fail to save image" ))
            }
        }

    })

}

func (iw *ImageWeb)Serve() {
    http.ListenAndServe("0.0.0.0:8080", nil)
}

