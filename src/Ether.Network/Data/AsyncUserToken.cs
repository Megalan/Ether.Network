using System;
using System.Net.Sockets;
using Ether.Network.Interfaces;

namespace Ether.Network.Data
{
    internal sealed class AsyncUserToken : IAsyncUserToken
    {
        public int ReceivedHeaderBytesCount { get; set; }

        public byte[] MessageHeaderData { get; set; }

        public int? MessageSize { get; set; }

        public int DataCursorIndex { get; set; }

        public int ReceivedMessageBytesCount { get; set; }

        public byte[] MessageData { get; set; }

        //

        public int DataStartOffset { get; set; }

        public int NextReceiveOffset { get; set; }

        public int TotalReceivedDataSize { get; set; }

        public byte[] TempData { get; set; }

        public Socket Socket { get; set; }

        public Action<byte[]> MessageHandler { get; set; }

        public AsyncUserToken()
            : this(null)
        {
        }

        public AsyncUserToken(Action<byte[]> messageHandlerAction)
        {
            this.ReceivedHeaderBytesCount = 0;
            this.MessageSize = null;
            this.DataCursorIndex = 0;

            this.MessageData = null;
            this.ReceivedMessageBytesCount = 0;
            //

            this.DataStartOffset = 0;
            this.NextReceiveOffset = 0;
            this.TotalReceivedDataSize = 0;
            this.MessageHandler = messageHandlerAction;
        }
    }
}