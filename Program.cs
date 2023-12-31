﻿
using System.IO.Compression;

namespace GZipTest
{
    internal static class Program
    {
        const int NUMBER_OF_TASKS = 20;
        const int DATA_BLOCK_SIZE = 32768; //1048576

        private static object _inputLocker = new object();
        private static object _outputLocker = new object();
        private static int _inputPos = 0;
        private static ulong _fileSize = 0;

        static async void ReadFile(CancellationTokenSource token, string fileName, QueueWraper inputQueue, CompressionMode mode)
        {
            try
            {
                //Console.WriteLine("The read task is started");
                FileStream originalFileStream;
                try
                {
                    originalFileStream = File.Open(fileName, FileMode.Open, FileAccess.Read);
                    _fileSize = (ulong)originalFileStream.Length / DATA_BLOCK_SIZE + 1;
                }
                catch (Exception ex)
                {
                    //Console.WriteLine("The read task is stopped");
                    Console.WriteLine(ex.Message);
                    token.Cancel();
                    return;
                }
                //int counter = 0;
                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        byte[]? buffer = null;
                        int localLength = 0;
                        if (inputQueue.Count >= NUMBER_OF_TASKS * 10)
                        {
                            try
                            {
                                await Task.Delay(1, token.Token);
                            }
                            catch
                            {
                                token.Cancel();
                                return;
                            }
                            continue;
                        }
                        if (mode == CompressionMode.Compress)
                        {
                            byte[] tmp = new byte[DATA_BLOCK_SIZE];

                            localLength = originalFileStream.Read(tmp, 0, DATA_BLOCK_SIZE);
                            if (localLength > 0)
                            {
                                buffer = new byte[localLength];
                                Array.Copy(tmp, 0, buffer, 0, localLength);
                                //counter++;
                                //if ((counter % 100) == 0)
                                //{
                                //    Console.WriteLine($"The number of read blocks is {counter}");
                                //}
                            }
                        }
                        else if (mode == CompressionMode.Decompress)
                        {
                            byte[] tmp = new byte[4];
                            localLength = originalFileStream.Read(tmp, 0, sizeof(int));
                            if (localLength <= 0)
                            {
                                lock (_inputLocker)
                                {
                                    inputQueue.m_NoDataForQueue = true;
                                }
                                originalFileStream.Dispose();
                                //Console.WriteLine($"The read task has finished work. The number of read blocks is {counter}");
                                return;
                            }
                            Array.Reverse(tmp);
                            localLength = BitConverter.ToInt32(tmp, 0);
                            buffer = new byte[localLength];
                            localLength = originalFileStream.Read(buffer, 0, localLength);
                            
                            //counter++;
                            //if ((counter % 100) == 0)
                            //{
                                //Console.WriteLine($"The number of read blocks is {counter}");
                            //}
                        }
                        if (localLength <= 0)
                        {
                            lock (_inputLocker)
                            {
                                inputQueue.m_NoDataForQueue = true;
                            }
                            originalFileStream.Dispose();
                            //Console.WriteLine($"The read task has finished work. The number of read blocks is {counter}");
                            return;
                        }
                        else
                        {
                            lock (_inputLocker)
                            {
                                inputQueue.Enqueue(buffer);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        lock (_inputLocker)
                        {
                            inputQueue.m_NoDataForQueue = true;
                        }
                        originalFileStream.Dispose();
                        //Console.WriteLine("The read task is stopped");
                        Console.WriteLine(ex.Message);
                        token.Cancel();
                        return;
                    }

                }
                originalFileStream.Dispose();
            }
            catch (Exception ex)
            {
                //Console.WriteLine("The read task is stopped");
                Console.WriteLine(ex.Message);
                token.Cancel();
            }
        }

        static async Task WriteFile(CancellationTokenSource token, string fileName, MapWraper outputMap, CompressionMode mode)
        {
            try
            {
                //Console.WriteLine("The write task is started");
                FileStream outputFile;
                ulong progress = 0;
                try
                {
                    outputFile = File.Create(fileName);
                }
                catch (Exception ex)
                {
                    //Console.WriteLine("The write task is stopped");
                    Console.WriteLine(ex.Message);
                    token.Cancel();
                    return;
                }
                
                bool isMapReaded = false;
                int runningTasks = 0;
                int currentPosition = 0;
                while (!token.IsCancellationRequested)
                {
                    try
                    { 
                        CompressedData data;
                        lock (_outputLocker)
                        {
                            isMapReaded = outputMap.TryGetValue(currentPosition, out data);
                            if (isMapReaded == true)
                            {
                                outputMap.Remove(currentPosition);
                            }
                            runningTasks = outputMap.m_RunningTasks;
                        }
                        if (isMapReaded == true)
                        {
                            try
                            {
                                if (mode == CompressionMode.Compress)
                                {
                                    byte[] intBytes = BitConverter.GetBytes(data.Length);
                                    Array.Reverse(intBytes);
                                    outputFile.Write(intBytes, 0, sizeof(int));
                                    outputFile.Write(data.Data, 0, data.Length);
                                }
                                else
                                {
                                    outputFile.Write(data.Data, 0, data.Data.Length);
                                }
                                //if ((currentPosition % 100) == 0)
                                //{
                                //    Console.WriteLine($"The number of written blocks is {currentPosition}");
                                //}
                                
                                
                                if (mode == CompressionMode.Compress)
                                {
                                    var tmp = (ulong)(100 * currentPosition) / _fileSize;
                                    if (tmp > progress)
                                    {
                                        progress = tmp;
                                        Console.Write($"\rCompression complete on {progress}%");
                                    }
                                }
                                else 
                                {
                                    if ((currentPosition % 4000) == 0)
                                    {
                                        Console.Write($"\rDecompression   ");
                                    }
                                    else if ((currentPosition % 4000) == 1000)
                                    {
                                        Console.Write($"\rDecompression.  ");
                                    }
                                    else if ((currentPosition % 4000) == 2000)
                                    {
                                        Console.Write($"\rDecompression.. ");
                                    }
                                    else if ((currentPosition % 4000) == 3000)
                                    {
                                        Console.Write($"\rDecompression...");
                                    }
                                }
                                                
                                currentPosition++;
                            }
                            catch (Exception ex)
                            {
                                //Console.WriteLine("The write task is stopped");
                                Console.WriteLine(ex.Message);
                                outputFile.Dispose();
                                token.Cancel();
                                return;
                            }
                        }
                        else if ((runningTasks == 0) && (outputMap.Count == 0))
                        {
                            //Console.WriteLine($"The write task has finished work. The number of written blocks is {currentPosition}");
                            if (mode == CompressionMode.Compress)
                            {
                                Console.Write($"\rCompression ");
                            }
                            else
                            {
                                Console.Write($"\rDecompression ");
                            }
                            Console.Write($"complete on 100%");
                            outputFile.Dispose();
                            return;
                        }
                        else
                        {
                            try
                            {
                                await Task.Delay(1, token.Token);
                            }
                            catch
                            {
                                token.Cancel();
                                return;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        //Console.WriteLine("The write task is stopped");
                        Console.WriteLine(ex.Message);
                        outputFile.Dispose();
                        token.Cancel();
                    }
                }
                outputFile.Dispose();
            }
            catch (Exception ex)
            {
                //Console.WriteLine("The write task is stopped");
                Console.WriteLine(ex.Message);
                token.Cancel();
            }
        }

        static async void Compress(CancellationTokenSource token, QueueWraper inputQueue, MapWraper outputMap, int id)
        {
            try
            {
                //Console.WriteLine($"Task {id} start compression");
                while (!token.IsCancellationRequested)
                {
                    int localInputPos = 0;
                    byte[]? buffer;
                    bool thereIsNoMoreData = false;
                    bool isQueueReaded = false;
                    lock (_inputLocker)
                    {
                        isQueueReaded = inputQueue.TryDequeue(out buffer);
                        thereIsNoMoreData = inputQueue.m_NoDataForQueue;
                        if (isQueueReaded == true)
                        {
                            localInputPos = _inputPos;
                            _inputPos++;
                        }
                    }
                    if (isQueueReaded == true)
                    {
                        try
                        {                           
                            using (MemoryStream destination = new MemoryStream(DATA_BLOCK_SIZE))
                            {
                                using (var compressor = new GZipStream(destination, CompressionMode.Compress, false))
                                {
                                    compressor.Write(buffer, 0, buffer.Length);                                    
                                }
                                CompressedData bufferToWrite = new CompressedData();
                                bufferToWrite.Data = destination.ToArray();
                                bufferToWrite.Length = bufferToWrite.Data.Length;

                                lock (_outputLocker)
                                {
                                    outputMap.Add(localInputPos, bufferToWrite);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            //Console.WriteLine($"The compression task {id} is stopped");
                            Console.WriteLine(ex.Message);
                            token.Cancel();
                            return;
                        }
                    }
                    else if (thereIsNoMoreData == false)
                    {
                        try
                        {
                            await Task.Delay(1, token.Token);
                        }
                        catch
                        {
                            token.Cancel();
                            return;
                        }
                        continue;
                    }
                    else if (inputQueue.Count == 0)
                    {
                        lock (_outputLocker)
                        {
                            outputMap.m_RunningTasks--;
                        }
                        break;
                    }
                }
                //Console.WriteLine($"Task {id} has finished compression");
            }
            catch (Exception ex)
            {
                //Console.WriteLine($"The compression task {id} is stopped");
                Console.WriteLine(ex.Message);
                token.Cancel();
            }
        }
        
        static async void Decompress(CancellationTokenSource token, QueueWraper inputQueue, MapWraper outputMap, int id)
        {
            try
            {
                //Console.WriteLine($"Task {id} start decompression");
                while (!token.IsCancellationRequested)
                {
                    int localInputPos = 0;
                    byte[]? buffer = new byte[DATA_BLOCK_SIZE * 2];
                    bool thereIsNoMoreData = false;
                    bool isQueueReaded = false;
                    lock (_inputLocker)
                    {
                        isQueueReaded = inputQueue.TryDequeue(out buffer);
                        thereIsNoMoreData = inputQueue.m_NoDataForQueue;
                        if (isQueueReaded == true)
                        {
                            localInputPos = _inputPos;
                            _inputPos++;
                        }
                    }
                    if (isQueueReaded == true)
                    {
                        try
                        {
                            CompressedData data = new CompressedData();
                            
                            using MemoryStream source = new MemoryStream(buffer);
                            using MemoryStream destination = new MemoryStream();
                            using (var decompressor = new GZipStream(source, CompressionMode.Decompress, false))
                            {
                                decompressor.CopyTo(destination);
                            }
                            lock (_outputLocker)
                            {
                                data.Data = destination.ToArray();
                                data.Length = data.Data.Length;
                                outputMap.Add(localInputPos, data);
                            }
                        }
                        catch (Exception ex)
                        {
                            //Console.WriteLine($"The decompression task {id} is stopped");
                            Console.WriteLine(ex.Message);
                            token.Cancel();
                            return;
                        }
                    }
                    else if (thereIsNoMoreData == false)
                    {
                        try
                        {
                            await Task.Delay(1, token.Token);
                        }
                        catch
                        {
                            token.Cancel();
                            return;
                        }
                        continue;
                    }
                    else if (inputQueue.Count == 0)
                    {
                        lock (_outputLocker)
                        {
                            outputMap.m_RunningTasks--;
                        }
                        break;
                    }
                }
                //Console.WriteLine($"Task {id} has finish decompression");
            }
            catch (Exception ex)
            {
                //Console.WriteLine($"The decompression task {id} is stopped");
                Console.WriteLine(ex.Message);
                token.Cancel();
            }
        }
        
        static async Task Main(string[] args)
        {
            try
            {
                if (args.Length < 2)
                {
                    Console.WriteLine("To compress you should call the program like showing below");
                    Console.WriteLine("GZipTest.exe compress[source file name] [archive name]");
                    Console.WriteLine("To decompress you should type next: ");
                    Console.WriteLine("GZipTest.exe decompress[source archive name] [output file name]");
                    return;
                }

                CancellationTokenSource _cts = new CancellationTokenSource();
                Console.CancelKeyPress +=
                            (sender, e) =>
                            {
                                Console.WriteLine("\nCtrl+C handling");

                                e.Cancel = true;
                                _cts.Cancel();
                            };
                QueueWraper inputQueue = new QueueWraper(DATA_BLOCK_SIZE * NUMBER_OF_TASKS * 2);
                MapWraper outputDictionary = new MapWraper(DATA_BLOCK_SIZE * NUMBER_OF_TASKS * 2, NUMBER_OF_TASKS);

                if (args[0] == "compress")
                {
                    try
                    {
                        Task.Run(() => { ReadFile(_cts, args[1], inputQueue, CompressionMode.Compress); });
                        for (var i = 0; i < NUMBER_OF_TASKS; i++)
                        {
                            var local = i;
                            Task.Run(() => { Compress(_cts, inputQueue, outputDictionary, local + 1);});
                        }
                        WriteFile(_cts, args[2], outputDictionary, CompressionMode.Compress).Wait();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex);
                    }
                }
                else if (args[0] == "decompress")
                {
                    try
                    {
                        Task.Run(() => { ReadFile(_cts, args[1], inputQueue, CompressionMode.Decompress); });
                        for (var i = 0; i < NUMBER_OF_TASKS; i++)
                        {
                            var local = i;
                            Task.Run(() => { Decompress(_cts, inputQueue, outputDictionary, local + 1); });
                        }
                        WriteFile(_cts, args[2], outputDictionary, CompressionMode.Decompress).Wait();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
                else
                {
                    Console.WriteLine("Unknown operation. To compress you should call the program show below");
                    Console.WriteLine("GZipTest.exe compress[source file name] [archive name]");
                    Console.WriteLine("To decompress you should type next: ");
                    Console.WriteLine("GZipTest.exe decompress[source archive name] [output file name]");
                    return;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
}