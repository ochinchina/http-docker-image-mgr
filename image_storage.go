package main

import (
	"fmt"
	"github.com/fsouza/go-dockerclient"
	"gopkg.in/mgo.v2"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

type ImageNameList struct {
    //all the names
    nameList []string

    //name map for avoiding duplicate name
    nameMap map[string]string
}

func NewImageNameList() *ImageNameList {
    return &ImageNameList{ nameList: make( []string, 0 ),
                nameMap: make(map[string]string) }
}

// add a image name and if the image already exists
// an error will be return
func (inl *ImageNameList)Add( name string) error {
    if _, ok := inl.nameMap[name]; ok {
        return fmt.Errorf( "%s already exists", name )
    }
    inl.nameMap[name] = name
    inl.nameList = append( inl.nameList, name )
    return nil
}

// get all the image names
func (inl *ImageNameList)Names() []string {
    return inl.nameList
}

func (inl *ImageNameList)Remove( name string) error {
    if _, ok := inl.nameMap[name]; ok {
        delete (inl.nameMap,name)
        for i, image_name := range inl.nameList {
            n := len( inl.nameList )
            if name == image_name {
                //swap the last with this
                inl.nameList[i], inl.nameList[ n - 1 ] = inl.nameList[n-1], inl.nameList[i]
                inl.nameList = inl.nameList[0:n-1]
                return nil
            }
        }
    }
    return fmt.Errorf( "image %s is not found", name )
}

type ImageStorage interface {
    // write image with name, 
    // the image itself can be read from reader
	Write(name string, reader io.Reader ) error

	// Get the image with name from the storage
    // and the image itself will be written to writer
	Get(name string, writer io.Writer ) error

    // Delete image by name
    Delete(name string) error

    // Get all the images in the storage
	List() ([]string, error)
}

func parseImageName( name string ) (string, string ) {
    pos := strings.Index(name, ":")

    if pos == -1 {
        return name, "latest"
    }
    return name[0:pos], name[pos+1:]
}

type FileImageStorage struct {
	Dir string
    images *ImageNameList
}

func NewFileImageStorage(dir string) *FileImageStorage {
    fis := &FileImageStorage{Dir: dir, images: NewImageNameList() }
    fis.loadImageNames()
    return fis
}

func (fis *FileImageStorage) Write(name string, reader io.Reader ) error {
	image_name, image_version := parseImageName( name )

	abs_dir := fmt.Sprintf("%s/%s", fis.Dir, image_name)
	err := os.MkdirAll(abs_dir, 0777)
	if err != nil {
		return err
	}

    //create the file
    f, err := os.Create(fmt.Sprintf("%s/%s", abs_dir, image_version))
    if err != nil {
        return err
    }
    defer f.Close()
    _, err = io.Copy( f, reader )
    if err == nil {
        fis.images.Add( fmt.Sprintf( "%s:%s", image_name, image_version ) )
    }
    return err

}

func (fis *FileImageStorage) Get(name string, writer io.Writer ) error {
	image_name, image_version := parseImageName( name )
    r, err := os.Open(fmt.Sprintf("%s/%s/%s", fis.Dir, image_name, image_version))

    if err != nil {
        return err
    }
    defer r.Close()
    _, err = io.Copy( writer, r )
    return err
}

func (fis *FileImageStorage)List()( []string, error ) {
    return fis.images.Names(), nil
}

func (fis *FileImageStorage)Delete( name string ) error {
    image_name, image_version := parseImageName( name )
    err := os.Remove( fmt.Sprintf("%s/%s/%s", fis.Dir, image_name, image_version) )
    if err == nil {
        fis.images.Remove( fmt.Sprintf( "%s:%s", image_name, image_version ) )
    }
    return err
}

func (fis *FileImageStorage) loadImageNames() error {
	files, err := ioutil.ReadDir(fis.Dir)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			fName := path.Join(fis.Dir, file.Name())
			version_files, err := ioutil.ReadDir(fName)
			if err == nil {
				for _, vf := range version_files {
					if !vf.IsDir() {
                        fis.images.Add( fmt.Sprintf("%s:%s", file.Name(), vf.Name()) )
					}
				}
			}
		}
	}

	return nil
}



type DockerImageStorage struct {
	client *docker.Client
}

func NewDockerImageStorage(client *docker.Client) *DockerImageStorage {
	return &DockerImageStorage{client: client}
}

func (dis *DockerImageStorage) Write(name string, reader io.Reader ) error {
    return dis.client.LoadImage(docker.LoadImageOptions{InputStream: reader })
}

func (dis *DockerImageStorage) Get(name string, writer io.Writer ) error {
    return dis.client.ExportImages(docker.ExportImagesOptions{Names: []string{name}, OutputStream: writer})
}

func (dis *DockerImageStorage)Delete( name string) error {
    return dis.client.RemoveImage( name )
}

func (dis *DockerImageStorage) List() ([]string, error) {
	result := make([]string, 0)
	imgs, err := dis.client.ListImages(docker.ListImagesOptions{All: false})
	if err != nil {
		return result, err
	}

	for _, img := range imgs {
        for _, name := range img.RepoTags {
            //discard the <none> image
            if strings.HasPrefix( name, "<none>" ) || strings.HasSuffix( name, ":<none>" ) {
                continue
            }
            result = append( result, name )
        }
	}
	return result, nil
}

type MongoImageStorage struct {
	url      string
	db       string
	fsPrefix string
    images *ImageNameList
}

type MongoFileIndex struct {
	UploadDate string
	Length     int
	Md5        string
	Filename   string
}

func NewMongoImageStorage(url string, db string, fsPrefix string) *MongoImageStorage {
    mis := &MongoImageStorage{url: url,
            db: db,
            fsPrefix: fsPrefix,
            images: NewImageNameList() }
    mis.loadImageNames()
    return mis
}

func (mis *MongoImageStorage) Get(name string, writer io.Writer ) error {
	session, fs, err := mis.createGridFS()
	if err != nil {
		return err
	}

	file, err := fs.Open(name)
	if err != nil {
		return err
	}

    defer file.Close()
    defer session.Close()

    _, err = io.Copy( writer, file )
    return err

}

func (mis *MongoImageStorage) List()([]string, error ) {
    return mis.images.Names(), nil
}

func (mis *MongoImageStorage) Write(name string, reader io.Reader ) error {

	session, fs, err := mis.createGridFS()
	if err != nil {
		return err
	}
    defer session.Close()

	file, err := fs.Open(name)
	if err != nil {
		return err
	}

    defer file.Close()
    defer session.Close()

    _, err = io.Copy( file, reader )

    if err == nil {
        mis.images.Add( name )
    }
    return err

}

func (mis *MongoImageStorage)Remove( name string ) error {
    session, fs, err := mis.createGridFS()
    if err != nil {
        return err
    }

    defer session.Close()

    err = fs.Remove( name )
    if err == nil {
        mis.images.Remove( name )
    }
    return err
}

func (mis *MongoImageStorage) loadImageNames() error {
	session, fs, err := mis.createGridFS()
	if err != nil {
		return err
	}

	defer session.Close()

    iter := fs.Find(nil).Iter()
    for {
        mongoFile := MongoFileIndex{}
        if !iter.Next( &mongoFile) {
            break
        }
        mis.images.Add( mongoFile.Filename )

    }
	return nil
}

func (mis *MongoImageStorage) createGridFS() (*mgo.Session, *mgo.GridFS, error) {
	session, err := mgo.Dial(mis.url)
	if err != nil {
		return nil, nil, err
	}

	db := session.DB(mis.db)
	fs := db.GridFS(mis.fsPrefix)
	return session, fs, err
}

