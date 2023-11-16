
namespace GZipTest
{
    internal class MapWraper: Dictionary<int, CompressedData>
    {
        internal int m_RunningTasks { get; set; } = 0;
        internal MapWraper(int size, int taskCount) : base(size) 
        {
            m_RunningTasks = taskCount; 
        }
    }
}
