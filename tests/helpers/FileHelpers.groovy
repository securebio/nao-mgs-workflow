/**
 * Test helper functions for file operations
 */

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

// Return this for use in other scripts
this
