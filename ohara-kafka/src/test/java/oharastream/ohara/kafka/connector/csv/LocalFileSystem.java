/*
 * Copyright 2019 is-land
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package oharastream.ohara.kafka.connector.csv;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import oharastream.ohara.common.exception.FileSystemException;
import oharastream.ohara.common.exception.NoSuchFileException;
import oharastream.ohara.kafka.connector.storage.FileSystem;
import oharastream.ohara.kafka.connector.storage.FileType;

public class LocalFileSystem implements FileSystem {
  public static LocalFileSystem of() {
    return new LocalFileSystem();
  }

  @Override
  public boolean exists(String path) {
    return Files.exists(Paths.get(path));
  }

  @Override
  public Iterator<String> listFileNames(String dir) {
    try {
      if (!exists(dir)) {
        throw new NoSuchFileException(String.format("The path %s doesn't exist", dir));
      }
      return Files.list(Paths.get(dir)).map(f -> f.getFileName().toString()).iterator();
    } catch (IOException e) {
      throw new FileSystemException(e);
    }
  }

  @Override
  public FileType fileType(String path) {
    return (new File(path)).isDirectory() ? FileType.FOLDER : FileType.FILE;
  }

  @Override
  public OutputStream create(String path) {
    try {
      if (exists(path)) {
        throw new IllegalArgumentException(String.format("The path %s already exists", path));
      }
      Path parent = Paths.get(path).getParent();
      if (parent != null && !exists(parent.toString())) {
        mkdirs(parent.toString());
      }
      return Files.newOutputStream(Paths.get(path));
    } catch (IOException e) {
      throw new FileSystemException(e);
    }
  }

  @Override
  public OutputStream append(String path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream open(String path) {
    try {
      if (!exists(path)) {
        throw new NoSuchFileException(String.format("The path %s doesn't exist", path));
      }
      return Files.newInputStream(Paths.get(path));
    } catch (IOException e) {
      throw new FileSystemException(e);
    }
  }

  public boolean copy(boolean delSrc, boolean overwrite, String sourcePath, String targetPath) {
    try {
      Path copied = Paths.get(targetPath);
      Path originalPath = Paths.get(sourcePath);

      if (overwrite) {
        Files.copy(originalPath, copied, StandardCopyOption.REPLACE_EXISTING);
      } else {
        Files.copy(originalPath, copied);
      }

      if (delSrc) {
        Files.delete(originalPath);
      }

      return true;
    } catch (IOException e) {
      throw new FileSystemException(e);
    }
  }

  @Override
  public boolean moveFile(String sourcePath, String targetPath) {
    try {
      if (!exists(sourcePath)) {
        throw new NoSuchFileException(
            String.format("The source path %s doesn't exist", sourcePath));
      }

      if (exists(targetPath)) {
        throw new IllegalArgumentException(
            String.format("The target path %s already exists", targetPath));
      }

      if (sourcePath.equals(targetPath)) {
        return false;
      }

      Files.move(Paths.get(sourcePath), Paths.get(targetPath));
      return exists(targetPath);
    } catch (IOException e) {
      throw new FileSystemException(e);
    }
  }

  @Override
  public void delete(String path) {
    delete(path, false);
  }

  @Override
  public void delete(String path, boolean recursive) {
    try {
      if (exists(path)) {
        if (recursive) {
          File file = Paths.get(path).toFile();
          if (file.isDirectory()) {
            File[] children = file.listFiles();
            for (File child : children) delete(child.toString(), true);
          }
        }
        Files.delete(Paths.get(path));
      }
    } catch (IOException e) {
      throw new FileSystemException(e);
    }
  }

  @Override
  public void mkdirs(String dir) {
    try {
      Files.createDirectories(Paths.get(dir));
    } catch (IOException e) {
      throw new FileSystemException(e);
    }
  }

  @Override
  public void close() {
    // Do nothing
  }
}
