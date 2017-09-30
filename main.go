package main

import (
	"encoding/json"
	"fmt"
	"github.com/fsouza/go-dockerclient"
	"gopkg.in/mgo.v2"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strings"
)

type ImageStorage interface {
	Write(name string) (io.WriteCloser, error)

	// Get the image from the storage
	// name the image name
	Get(name string) (io.ReadCloser, error)

	List() ([]string, error)
}

type FileImageStorage struct {
	Dir string
}

func NewFileImageStorage(dir string) *FileImageStorage {
	return &FileImageStorage{Dir: dir}
}

func (fis *FileImageStorage) Write(name string) (io.WriteCloser, error) {
	image_name := ""
	image_version := "latest"
	pos := strings.Index(name, ":")

	if pos == -1 {
		image_name = name
	} else {
		image_name = name[0:pos]
		image_version = name[pos+1:]
	}

	abs_dir := fmt.Sprintf("%s/%s", fis.Dir, image_name)
	err := os.MkdirAll(abs_dir, 0777)
	if err != nil {
		return nil, err
	}
	return os.Create(fmt.Sprintf("%s/%s", abs_dir, image_version))

}

func (fis *FileImageStorage) Get(name string) (io.ReadCloser, error) {
	image_name := ""
	image_version := "latest"
	pos := strings.Index(name, ":")

	if pos == -1 {
		image_name = name
	} else {
		image_name = name[0:pos]
		image_version = name[pos+1:]
	}
	return os.Open(fmt.Sprintf("%s/%s/%s", fis.Dir, image_name, image_version))
}

func (fis *FileImageStorage) List() ([]string, error) {
	files, err := ioutil.ReadDir(fis.Dir)
	result := make([]string, 0)
	if err != nil {
		return result, err
	}

	for _, file := range files {
		if file.IsDir() {
			fName := path.Join(fis.Dir, file.Name())
			version_files, err := ioutil.ReadDir(fName)
			if err == nil {
				for _, vf := range version_files {
					if !vf.IsDir() {
						result = append(result, fmt.Sprintf("%s:%s", file.Name(), vf.Name()))
					}
				}
			}
		}
	}

	return result, nil

}

type DockerImageStorage struct {
	client *docker.Client
}

func NewDockerImageStorage(client *docker.Client) *DockerImageStorage {
	return &DockerImageStorage{client: client}
}

func (dis *DockerImageStorage) Write(name string) (io.WriteCloser, error) {
	r, w := io.Pipe()

	go func() {
		dis.client.LoadImage(docker.LoadImageOptions{InputStream: r})
	}()
	return w, nil
}

func (dis *DockerImageStorage) Get(name string) (io.ReadCloser, error) {
	fmt.Printf("start to save the image %s\n", name)
	r, w := io.Pipe()

	//avoid write block
	//start to thread to export the image
	go func() {
		defer w.Close()
		dis.client.ExportImages(docker.ExportImagesOptions{Names: []string{name}, OutputStream: w})

	}()

	return r, nil
}

func (dis *DockerImageStorage) List() ([]string, error) {
	result := make([]string, 0)
	imgs, err := dis.client.ListImages(docker.ListImagesOptions{All: false})
	if err != nil {
		return result, err
	}

	for _, img := range imgs {
		tmp := make([]string, len(result)+len(img.RepoTags))
		copy(tmp[0:len(result)], result)
		copy(tmp[len(result):], img.RepoTags)
		result = tmp
	}
	return result, nil
}

type MogoImageStorage struct {
	url      string
	db       string
	fsPrefix string
}

func NewMogoImageStorage(url string, db string, fsPrefix string) *MogoImageStorage {
	return &MogoImageStorage{url: url, db: db, fsPrefix: fsPrefix}
}

func (mis *MogoImageStorage) Get(name string) (io.ReadCloser, error) {
	session, fs, err := mis.createGridFS()
	if err != nil {
		return nil, err
	}

	file, err := fs.Open(name)
	if err != nil {
		return nil, err
	}

	r, w := io.Pipe()
	go func() {
		defer file.Close()
		defer session.Close()
		io.Copy(w, file)
	}()

	return r, nil

}

func (mis *MogoImageStorage) Write(name string) (io.WriteCloser, error) {

	session, fs, err := mis.createGridFS()
	if err != nil {
		return nil, err
	}

	file, err := fs.Open(name)
	if err != nil {
		return nil, err
	}

	r, w := io.Pipe()
	go func() {
		defer file.Close()
		defer session.Close()
		io.Copy(file, r)
	}()

	return w, nil
}

func (mis *MogoImageStorage) List() ([]string, error) {
	session, fs, err := mis.createGridFS()
	if err != nil {
		return nil, err
	}

	defer session.Close()

	_, err = fs.Open(".test")

	return make([]string, 0), err
}

func (mis *MogoImageStorage) createGridFS() (*mgo.Session, *mgo.GridFS, error) {
	session, err := mgo.Dial(mis.url)
	if err != nil {
		return nil, nil, err
	}

	db := session.DB(mis.db)
	fs := db.GridFS(mis.fsPrefix)
	return session, fs, err
}

func save_image(client *docker.Client, name string, imageStorage ImageStorage) error {
	fout, err := imageStorage.Write(name)
	if err != nil {
		return err
	}
	defer fout.Close()
	return client.ExportImages(docker.ExportImagesOptions{Names: []string{name}, OutputStream: fout})
}

func load_image(client docker.Client, in io.Reader) error {
	return client.LoadImage(docker.LoadImageOptions{InputStream: in})
}

func load_image_file(client docker.Client, file_name string) error {
	file_in, err := os.Open(file_name)
	if err != nil {
		return err
	}
	return load_image(client, file_in)
}

func main() {
	endpoint := "unix:///var/run/docker.sock"
	client, err := docker.NewClient(endpoint)
	if err != nil {
		panic(err)
	}
	imgs, err := client.ListImages(docker.ListImagesOptions{All: false})
	if err != nil {
		panic(err)
	}
	//image_storage := NewFileImageStorage( ".")
	image_storage := NewDockerImageStorage(client)
	http.HandleFunc("/image/get/", func(rw http.ResponseWriter, req *http.Request) {
		a := strings.Split(req.URL.Path, "/")
		fmt.Printf("%q\n", a)
		r, err := image_storage.Get(a[len(a)-1])
		if err == nil {
			io.Copy(rw, r)
			r.Close()
		}

	})

	http.HandleFunc("/image/list", func(rw http.ResponseWriter, req *http.Request) {
		if images, err := image_storage.List(); err == nil {
			rw.Header().Set("Content-Type", "application/json") // normal header
			if b, err := json.Marshal(images); err == nil {
				rw.Write(b)
			}
		}

	})

	http.HandleFunc("/image/save/", func(rw http.ResponseWriter, req *http.Request) {
		a := strings.Split(req.URL.Path, "/")
		if req.Method == "POST" {
			defer req.Body.Close()
			n := len(a)
			w, err := image_storage.Write(a[n-2] + ":" + a[n-1])
			if err == nil {
				defer w.Close()
				_, err = io.Copy(w, req.Body)
			}
		}

	})

	http.ListenAndServe("0.0.0.0:8080", nil)
	/*redis_file, err :=os.Create("redis_latest.tar")
	  if err != nil {
	      panic(err)
	  }
	  client.ExportImages( docker.ExportImagesOptions{Names:[]string{"redis:latest"}, OutputStream: redis_file})


	  redis_file.Close()*/
	save_image(client, "redis:latest", image_storage)
	for _, img := range imgs {
		fmt.Println("ID: ", img.ID)
		fmt.Println("RepoTags: ", img.RepoTags)
		fmt.Println("Created: ", img.Created)
		fmt.Println("Size: ", img.Size)
		fmt.Println("VirtualSize: ", img.VirtualSize)
		fmt.Println("ParentId: ", img.ParentID)
	}
}
