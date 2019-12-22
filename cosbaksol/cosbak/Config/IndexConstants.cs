namespace Cosbak.Config
{
    public class IndexConstants
    {
        public int MaxLogBufferSize { get; set; } = 8 * 1024 * 1024;
        
        public int MaxIndexBufferSize { get; set; } = 4 * 1024 * 1024;
        
        public int MaxContentBufferSize { get; set; } = 32 * 1024 * 1024;
    }
}