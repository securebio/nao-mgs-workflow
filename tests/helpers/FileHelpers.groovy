/**
 * Test helper functions for file operations
 */

import java.util.zip.GZIPOutputStream
import java.nio.file.Files

/**
 * Checks if a file is gzipped by reading the magic bytes
 * @param file The file to check
 * @return true if the file is gzipped, false otherwise
 */
def isGzipped(file) {
    def firstByte, secondByte
    new File(file.toString()).withInputStream { fis ->
        firstByte = fis.read()
        secondByte = fis.read()
    }
    return firstByte == 0x1f && secondByte == 0x8b
}

/**
 * Gzip every file in `srcPath` into a fresh temp directory (preserving the
 * source dir's basename so any companion paths.csv-style file remains valid)
 * and return the resulting directory path. Lets fixtures be committed
 * plain-text (legible to humans inspecting test-data/) but supplied to
 * processes in their production .gz form.
 * @param srcPath Path to a directory of plain-text fixture files
 * @return Absolute path to a new directory whose contents are .gz copies
 */
def gzipFixtureDir(srcPath) {
    def src = new File(srcPath.toString())
    def dst = new File(Files.createTempDirectory("nft-gzip-").toFile(), src.name)
    dst.mkdirs()
    src.eachFile { f ->
        def out = new File(dst, "${f.name}.gz")
        new GZIPOutputStream(out.newOutputStream()).withCloseable { gz ->
            gz.write(f.bytes)
        }
    }
    return dst.getAbsolutePath()
}

// Return this for use in other scripts
this
