using Ether.Network.Interfaces;
using System;
using System.Net.Sockets;

namespace Ether.Network.Utils
{
    internal static class SocketAsyncUtils
    {
        public static void ProcessReceivedData(SocketAsyncEventArgs e, IAsyncUserToken token, IPacketProcessor packetProcessor, int alreadyProcessedDataSize)
        {
            if (e == null)
                throw new ArgumentNullException(nameof(e));

            int dataStartOffset = token.DataStartOffset;

            if (alreadyProcessedDataSize >= token.TotalReceivedDataSize)
                return;

            try
            {
                if (token.MessageSize == null)
                {
                    int headerSize = packetProcessor.HeaderSize;

                    if (token.ReceivedHeaderBytesCount == 0)
                    {
                    }

                    if (token.TotalReceivedDataSize > headerSize)
                    {
                        byte[] headerData = NetUtils.GetPacketBuffer(e.Buffer, dataStartOffset, headerSize);
                        int messageSize = packetProcessor.GetLength(headerData);

                        token.MessageSize = messageSize - headerSize;
                        token.DataStartOffset = dataStartOffset + headerSize;

                        ProcessReceivedData(e, token, packetProcessor, alreadyProcessedDataSize + headerSize);
                    }
                }
                else
                {
                    var messageSize = token.MessageSize.Value;
                    if (token.TotalReceivedDataSize - alreadyProcessedDataSize >= messageSize)
                    {
                        byte[] messageData = NetUtils.GetPacketBuffer(e.Buffer, dataStartOffset, messageSize);

                        token.MessageHandler?.Invoke(messageData);
                        token.DataStartOffset = dataStartOffset + messageSize;
                        token.MessageSize = null;

                        ProcessReceivedData(e, token, packetProcessor, alreadyProcessedDataSize + messageSize);
                    }
                }
            }
            catch (Exception ex)
            {
            }
        }

        public static void ProcessNextReceive(SocketAsyncEventArgs e, IAsyncUserToken token)
        {
            token.NextReceiveOffset += e.BytesTransferred;

            if (token.NextReceiveOffset == e.Buffer.Length)
            {
                token.NextReceiveOffset = 0;

                if (token.DataStartOffset < e.Buffer.Length)
                {
                    var notYesProcessDataSize = e.Buffer.Length - token.DataStartOffset;
                    Buffer.BlockCopy(e.Buffer, token.DataStartOffset, e.Buffer, 0, notYesProcessDataSize);

                    token.NextReceiveOffset = notYesProcessDataSize;
                }

                token.DataStartOffset = 0;
            }

            e.SetBuffer(token.NextReceiveOffset, e.Buffer.Length - token.NextReceiveOffset);
        }
    }
}
