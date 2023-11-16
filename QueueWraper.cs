
namespace GZipTest
{
    internal class QueueWraper: Queue<byte[]>
    {
        internal bool m_NoDataForQueue  { get; set; } = false;
        internal QueueWraper(int size): base(size){ }
    }
}
