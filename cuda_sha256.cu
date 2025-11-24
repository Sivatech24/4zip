// Simple CUDA SHA-256 wrapper
extern "C" {
    void gpu_sha256(const unsigned char* input, size_t len, unsigned char* output) {
        // TEMP: GPU SHA256 is not implemented.
        // Fallback will be CPU SHA256.
        // Replace later with real CUDA implementation.
    }
}
