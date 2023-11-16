
using System.IO.Compression;

namespace GZipTest
{
    internal struct CompressedData
    {
        public int Length { get; set; } = 0;
        public byte[]? Data { get; set; }
        public CompressionMode Mode { get; set; }
        public CompressedData(int lenght, byte[] data)
        { 
            Length = lenght;
            Data = data;
        }
        public CompressedData() { }
    }
}
