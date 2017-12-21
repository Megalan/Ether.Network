using Ether.Network.Data;
using Ether.Network.Exceptions;
using Ether.Network.Interfaces;
using Ether.Network.Packets;
using Ether.Network.Server;
using Ether.Network.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Ether.Network
{
    /// <summary>
    /// Creates a new TCP managed server.
    /// </summary>
    public abstract class NetServer<T> : NetConnection, INetServer, IDisposable where T : NetUser, new()
    {
        private static readonly IPacketProcessor DefaultPacketProcessor = new NetPacketProcessor();
        private static readonly string AllInterfaces = "0.0.0.0";

        private readonly ManualResetEvent _manualResetEvent;
        private readonly AutoResetEvent _autoSendEvent;
        private readonly ConcurrentDictionary<Guid, T> _clients;
        private readonly BlockingCollection<MessageData> _messageQueue;
        private readonly SocketAsyncEventArgsPool _readPool;
        private readonly SocketAsyncEventArgsPool _writePool;
        private readonly Task _sendQueueTask;

        private bool _isDisposed;

        /// <summary>
        /// Gets the <see cref="NetServer{T}"/> configuration
        /// </summary>
        protected NetServerConfiguration Configuration { get; }

        /// <summary>
        /// Gets the packet processor.
        /// </summary>
        protected virtual IPacketProcessor PacketProcessor => DefaultPacketProcessor;

        /// <summary>
        /// Gets the <see cref="NetServer{T}"/> running state.
        /// </summary>
        public bool IsRunning { get; private set; }

        /// <summary>
        /// Gets the connected client.
        /// </summary>
        public IReadOnlyCollection<T> Clients => this._clients.Values as IReadOnlyCollection<T>;

        /// <summary>
        /// Creates a new <see cref="NetServer{T}"/> instance.
        /// </summary>
        protected NetServer()
        {
            this.Configuration = new NetServerConfiguration(this);
            this.Socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            this._clients = new ConcurrentDictionary<Guid, T>();
            this._messageQueue = new BlockingCollection<MessageData>();
            this._readPool = new SocketAsyncEventArgsPool();
            this._writePool = new SocketAsyncEventArgsPool();

            this._manualResetEvent = new ManualResetEvent(false);
            this._autoSendEvent = new AutoResetEvent(false);
            this._sendQueueTask = new Task(this.ProcessSendQueue);
        }

        /// <summary>
        /// Initialize and start the server.
        /// </summary>
        public void Start()
        {
            if (this.IsRunning)
                throw new InvalidOperationException("Server is already running.");

            if (this.Configuration.Port <= 0)
                throw new EtherConfigurationException($"{this.Configuration.Port} is not a valid port.");

            var address = this.Configuration.Host == AllInterfaces ? IPAddress.Any : this.Configuration.Address;
            if (address == null)
                throw new EtherConfigurationException($"Invalid host : {this.Configuration.Host}");

            for (int i = 0; i < this.Configuration.MaximumNumberOfConnections; i++)
            {
                this._readPool.Push(NetUtils.CreateSocketAsync(null, this.Configuration.BufferSize, this.IO_Completed));
                this._writePool.Push(NetUtils.CreateSocketAsync(null, this.Configuration.BufferSize, this.IO_Completed));
            }

            this.Initialize();
            this.Socket.Bind(new IPEndPoint(address, this.Configuration.Port));
            this.Socket.Listen(this.Configuration.Backlog);
            this.StartAccept(NetUtils.CreateSocketAsync(null, -1, this.IO_Completed));

            this.IsRunning = true;
            this._sendQueueTask.Start();
            this._manualResetEvent.WaitOne();
        }

        /// <summary>
        /// Stop the server.
        /// </summary>
        public void Stop()
        {
            if (this.IsRunning)
            {
                this.IsRunning = false;
                this._manualResetEvent.Set();
            }
        }

        /// <summary>
        /// Disconnects the client from this server.
        /// </summary>
        /// <param name="clientId">Client unique Id</param>
        public void DisconnectClient(Guid clientId)
        {
            if (!this._clients.ContainsKey(clientId))
                throw new EtherClientNotFoundException(clientId);

            if (this._clients.TryRemove(clientId, out T removedClient))
            {
                removedClient.Dispose();
                this.OnClientDisconnected(removedClient);
            }
        }

        /// <summary>
        /// Initialize the server resourrces.
        /// </summary>
        protected abstract void Initialize();

        /// <summary>
        /// Triggered when a new client is connected to the server.
        /// </summary>
        /// <param name="connection"></param>
        protected abstract void OnClientConnected(T connection);

        /// <summary>
        /// Triggered when a client disconnects from the server.
        /// </summary>
        /// <param name="connection"></param>
        protected abstract void OnClientDisconnected(T connection);

        /// <summary>
        /// Triggered when an error occurs on the server.
        /// </summary>
        /// <param name="exception">Exception</param>
        protected abstract void OnError(Exception exception);

        /// <summary>
        /// Starts the accept connection async operation.
        /// </summary>
        private void StartAccept(SocketAsyncEventArgs e)
        {
            if (e.AcceptSocket != null)
                e.AcceptSocket = null;

            if (!this.Socket.AcceptAsync(e))
                this.ProcessAccept(e);
        }

        /// <summary>
        /// Process the accept connection async operation.
        /// </summary>
        /// <param name="e"></param>
        private void ProcessAccept(SocketAsyncEventArgs e)
        {
            try
            {
                if (e.SocketError == SocketError.Success)
                {
                    SocketAsyncEventArgs readArgs = this._readPool.Pop();

                    if (readArgs != null)
                    {
                        var client = new T
                        {
                            Socket = e.AcceptSocket
                        };
                        client.SendAction = this.SendMessageAction;
                        client.Token.Socket = client.Socket;
                        client.Token.MessageHandler = messageData => this.HandleIncomingMessages(client, messageData);

                        if (!this._clients.TryAdd(client.Id, client))
                            throw new EtherException($"Client {client.Id} already exists in client list.");

                        this.OnClientConnected(client);
                        readArgs.UserToken = client;

                        if (!e.AcceptSocket.ReceiveAsync(readArgs))
                            this.ProcessReceive(readArgs);
                    }
                }
            }
            catch (Exception exception)
            {
                this.OnError(exception);
            }
            finally
            {
                this.StartAccept(e);
            }
        }

        /// <summary>
        /// Process the send async operation.
        /// </summary>
        /// <param name="e"></param>
        private void ProcessSend(SocketAsyncEventArgs e)
        {
            this._writePool.Push(e);
            this._autoSendEvent.Set();
        }

        /// <summary>
        /// Adds the message to the sending queue.
        /// </summary>
        /// <param name="user">User that sent the message</param>
        /// <param name="message">Message</param>
        private void SendMessageAction(INetUser user, byte[] message)
        {
            this._messageQueue.Add(new MessageData(user, message));
        }

        /// <summary>
        /// Process the send queue.
        /// </summary>
        private void ProcessSendQueue()
        {
            while (true)
            {
                var message = this._messageQueue.Take();

                if (message.User != null && message.Message != null)
                    this.SendMessage(message);
            }
        }

        /// <summary>
        /// Sends the message through the network.
        /// </summary>
        /// <param name="messageData"></param>
        private void SendMessage(MessageData messageData)
        {
            var writeEventArgs = this._writePool.Pop();

            if (writeEventArgs != null)
            {
                writeEventArgs.SetBuffer(messageData.Message, 0, messageData.Message.Length);
                writeEventArgs.UserToken = messageData.User;
                messageData.User.Socket.SendAsync(writeEventArgs);
            }
            else
            {
                this._autoSendEvent.WaitOne();
                this.SendMessage(messageData);
            }
        }

        /// <summary>
        /// Process receieve.
        /// </summary>
        /// <param name="e"></param>
        private void ProcessReceive(SocketAsyncEventArgs e)
        {
            if (e.SocketError == SocketError.Success && e.BytesTransferred > 0)
            {
                var connection = e.UserToken as NetUser;
                IAsyncUserToken token = connection.Token;

                int totalReceived = e.BytesTransferred;

                // Read header
                if (token.MessageSize == null)
                {
                    int headerSize = this.PacketProcessor.HeaderSize;

                    if (token.MessageHeaderData != null && token.ReceivedHeaderBytesCount < headerSize)
                    {
                        // Read the rest of the header
                        int restOfHeaderLength = headerSize - token.ReceivedHeaderBytesCount;
                        byte[] restOfHeader = NetUtils.GetPacketBuffer(e.Buffer, token.DataCursorIndex, restOfHeaderLength);

                        token.MessageHeaderData = token.MessageHeaderData.Concat(restOfHeader).ToArray();
                        token.ReceivedHeaderBytesCount = token.MessageHeaderData.Length;
                        token.DataCursorIndex += restOfHeaderLength;
                    }
                    else if (totalReceived > token.DataCursorIndex + headerSize)
                    {
                        // Read all the header
                        token.MessageHeaderData = NetUtils.GetPacketBuffer(e.Buffer, token.DataCursorIndex, headerSize);
                        token.ReceivedHeaderBytesCount = headerSize;
                        token.DataCursorIndex += headerSize;
                    }
                    else
                    {
                        // Read only one part of the header
                        int remainingBytesInThisBuffer = totalReceived - token.DataCursorIndex;
                        token.MessageHeaderData = NetUtils.GetPacketBuffer(e.Buffer, token.DataCursorIndex, remainingBytesInThisBuffer);
                        token.ReceivedHeaderBytesCount = token.MessageHeaderData.Length;
                        token.DataCursorIndex = 0;
                    }

                    if (token.ReceivedHeaderBytesCount == headerSize)
                    {
                        int messageSize = this.PacketProcessor.GetLength(token.MessageHeaderData);

                        Console.WriteLine("Received Message size : {0}", messageSize);
                        token.MessageHeaderData = null;
                        token.ReceivedHeaderBytesCount = 0;
                        token.MessageSize = messageSize - headerSize;
                    }
                    else
                    {
                        if (!token.Socket.ReceiveAsync(e))
                            this.ProcessReceive(e);
                        return;
                    }
                }

                // Read content
                if (token.MessageSize.HasValue)
                {
                    int messageSize = token.MessageSize.Value;

                    // TODO
                }


                //token.TotalReceivedDataSize = token.NextReceiveOffset - token.DataStartOffset + e.BytesTransferred;
                //SocketAsyncUtils.ProcessReceivedData(e, token, this.PacketProcessor, 0);
                //SocketAsyncUtils.ProcessNextReceive(e, token);

                if (!token.Socket.ReceiveAsync(e))
                    this.ProcessReceive(e);
            }
            else
            {
                this.CloseConnection(e);
            }
        }

        /// <summary>
        /// Close the connection.
        /// </summary>
        /// <param name="e"></param>
        private void CloseConnection(SocketAsyncEventArgs e)
        {
            var connection = e.UserToken as INetUser;

            connection.Dispose();
            this._readPool.Push(e);
            this.OnClientDisconnected(connection as T);
        }

        /// <summary>
        /// Handle incoming message packets.
        /// </summary>
        /// <param name="user">Current user</param>
        /// <param name="messageData">Incoming message data</param>
        private void HandleIncomingMessages(INetUser user, byte[] messageData)
        {
            using (var packet = this.PacketProcessor.CreatePacket(messageData))
                user.HandleMessage(packet);
        }
        
        /// <summary>
        /// Triggered when a <see cref="SocketAsyncEventArgs"/> async operation is completed.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void IO_Completed(object sender, SocketAsyncEventArgs e)
        {
            if (sender == null)
                throw new ArgumentNullException(nameof(sender));

            switch (e.LastOperation)
            {
                case SocketAsyncOperation.Accept:
                    this.ProcessAccept(e);
                    break;
                case SocketAsyncOperation.Receive:
                    this.ProcessReceive(e);
                    break;
                case SocketAsyncOperation.Send:
                    this.ProcessSend(e);
                    break;
                case SocketAsyncOperation.Disconnect: break;
                default:
                    throw new InvalidOperationException("Unexpected SocketAsyncOperation.");
            }
        }

        /// <summary>
        /// Dispose the <see cref="NetServer{T}"/> resources.
        /// </summary>
        /// <param name="disposing"></param>
        protected override void Dispose(bool disposing)
        {
            if (!this._isDisposed)
            {
                if (disposing)
                {
                    this._readPool?.Dispose();
                    this._writePool?.Dispose();

                    foreach (var client in this._clients)
                        client.Value.Dispose();

                    this._clients.Clear();
                    this._isDisposed = true;
                }
            }

            base.Dispose(disposing);
        }
    }
}
