/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#import <Foundation/Foundation.h>
#import "TSocketServer.h"
#import "TNSFileHandleTransport.h"
#import "TProtocol.h"
#import "TTransportException.h"
#import "TObjective-C.h"
#import <sys/socket.h>
#include <netinet/in.h>
#include <sys/un.h>



NSString * const kTSocketServer_ClientConnectionFinishedForProcessorNotification = @"TSocketServer_ClientConnectionFinishedForProcessorNotification";
NSString * const kTSocketServer_ProcessorKey = @"TSocketServer_Processor";
NSString * const kTSockerServer_TransportKey = @"TSockerServer_Transport";

@interface TSocketServer ()
{
    NSString *mDomainSocketPath;
}

@end

@implementation TSocketServer

- (id) initWithPort: (int) port
    protocolFactory: (id <TProtocolFactory>) protocolFactory
   processorFactory: (id <TProcessorFactory>) processorFactory
{
  CFSocketRef socket = [self createSocketWithPort:port];
  if (socket == NULL) {
      return nil;
  }

  if (self = [self initWithSocket:socket protocolFactory:protocolFactory processorFactory:processorFactory]) {
      NSLog(@"Listening on TCP port %d", port);
  }
  return self;
}

- (CFSocketRef) createSocketWithPort: (int) port
{
  CFSocketRef socket = CFSocketCreate(kCFAllocatorDefault, PF_INET, SOCK_STREAM, IPPROTO_TCP, 0, NULL, NULL);
  if (socket) {
    CFOptionFlags flagsToClear = kCFSocketCloseOnInvalidate;
    CFSocketSetSocketFlags(socket,  CFSocketGetSocketFlags(socket) & ~flagsToClear);

    int fd = CFSocketGetNative(socket);
    int yes = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (void *)&yes, sizeof(yes));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_len = sizeof(addr);
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    NSData *address = [NSData dataWithBytes:&addr length:sizeof(addr)];
    if (CFSocketSetAddress(socket, (bridge_stub CFDataRef)address) != kCFSocketSuccess) {
      CFSocketInvalidate(socket);
      CFRelease(socket);
      NSLog(@"*** Could not bind to address");
      return NULL;
    }

    return socket;
  } else {
    NSLog(@"*** No server socket");
    return NULL;
  }
}

- (id) initWithSocket: (CFSocketRef) socket
      protocolFactory: (id <TProtocolFactory>) protocolFactory
     processorFactory: (id <TProcessorFactory>) processorFactory
{
  self = [super init];

  mInputProtocolFactory = [protocolFactory retain_stub];
  mOutputProtocolFactory = [protocolFactory retain_stub];
  mProcessorFactory = [processorFactory retain_stub];

  // create a socket.
  int fd = CFSocketGetNative(socket);

  // wrap it in a file handle so we can get messages from it
  mSocketFileHandle = [[NSFileHandle alloc] initWithFileDescriptor: fd
                                                    closeOnDealloc: YES];
  
  // throw away our socket
  CFSocketInvalidate(socket);
  CFRelease(socket);
  
    // register for notifications of accepted incoming connections
  [[NSNotificationCenter defaultCenter] addObserver: self
                                           selector: @selector(connectionAccepted:)
                                               name: NSFileHandleConnectionAcceptedNotification
                                             object: mSocketFileHandle];
  
  // tell socket to listen
  [mSocketFileHandle acceptConnectionInBackgroundAndNotify];
  
  return self;
}

- (id) initWithPath: (NSString *) path
    protocolFactory: (id <TProtocolFactory>) protocolFactory
   processorFactory: (id <TProcessorFactory>) processorFactory
{
  mDomainSocketPath = path;
  CFSocketRef socket = [self createSocketWithPath:path];
  if (socket == NULL) {
      return nil;
  }

  if (self = [self initWithSocket:socket protocolFactory:protocolFactory processorFactory:processorFactory]) {
      NSLog(@"Listening on path %@", path);
  }
  return self;
}

- (CFSocketRef) createSocketWithPath: (NSString *) path
{
  CFSocketRef socket = CFSocketCreate(kCFAllocatorDefault, PF_LOCAL, SOCK_STREAM, IPPROTO_IP, 0, NULL, NULL);
  if (socket) {
    CFOptionFlags flagsToClear = kCFSocketCloseOnInvalidate;
    CFSocketSetSocketFlags(socket,  CFSocketGetSocketFlags(socket) & ~flagsToClear);

    int fd = CFSocketGetNative(socket);
    int yes = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (void *)&yes, sizeof(yes)) < 0) {
        NSLog(@"*** Unable to set REUSEADDR on socket.");
        return NULL;
    }

    size_t nullTerminatedPathLength = path.length + 1;
    struct sockaddr_un addr;
    if (nullTerminatedPathLength> sizeof(addr.sun_path)) {
        NSLog(@"*** Unable to create socket at path %@. Path is too long.", path);
        return NULL;
    }

    addr.sun_family = AF_LOCAL;
    memcpy(addr.sun_path, path.UTF8String, nullTerminatedPathLength);
    addr.sun_len = SUN_LEN(&addr);

    NSData *address = [NSData dataWithBytes:&addr length:sizeof(addr)];
    if (CFSocketSetAddress(socket, (bridge_stub CFDataRef)address) != kCFSocketSuccess) {
        CFSocketInvalidate(socket);
        CFRelease(socket);
        NSLog(@"*** Could not bind to address");
        return NULL;
    }
    
    return socket;
  } else {
    NSLog(@"*** No server socket");
    return NULL;
  }
}

- (void) dealloc {
  [[NSNotificationCenter defaultCenter] removeObserver:self];
  [mInputProtocolFactory release_stub];
  [mOutputProtocolFactory release_stub];
  [mProcessorFactory release_stub];
  [mSocketFileHandle release_stub];

  if (mDomainSocketPath != nil) {
    unlink(mDomainSocketPath.UTF8String);
  }

  [super dealloc_stub];
}


- (void) connectionAccepted: (NSNotification *) aNotification
{
  NSFileHandle * socket = [[aNotification userInfo] objectForKey: NSFileHandleNotificationFileHandleItem];

  // now that we have a client connected, spin off a thread to handle activity
  [NSThread detachNewThreadSelector: @selector(handleClientConnection:)
                           toTarget: self
                         withObject: socket];

  [[aNotification object] acceptConnectionInBackgroundAndNotify];
}


- (void) handleClientConnection: (NSFileHandle *) clientSocket
{
#if __has_feature(objc_arc)
    @autoreleasepool {
        TNSFileHandleTransport * transport = [[TNSFileHandleTransport alloc] initWithFileHandle: clientSocket];
        id<TProcessor> processor = [mProcessorFactory processorForTransport: transport];
        
        id <TProtocol> inProtocol = [mInputProtocolFactory newProtocolOnTransport: transport];
        id <TProtocol> outProtocol = [mOutputProtocolFactory newProtocolOnTransport: transport];
        
        @try {
            BOOL result = NO;
            do {
                @autoreleasepool {
                    result = [processor processOnInputProtocol: inProtocol outputProtocol: outProtocol];
                }
            } while (result);
        }
        @catch (TTransportException * te) {
            (void)te;
            //NSLog(@"Caught transport exception, abandoning client connection: %@", te);
        }
        
        NSNotification * n = [NSNotification notificationWithName: kTSocketServer_ClientConnectionFinishedForProcessorNotification
                                                           object: self
                                                         userInfo: [NSDictionary dictionaryWithObjectsAndKeys: 
                                                                    processor,
                                                                    kTSocketServer_ProcessorKey,
                                                                    transport,
                                                                    kTSockerServer_TransportKey,
                                                                    nil]];
        [[NSNotificationCenter defaultCenter] performSelectorOnMainThread: @selector(postNotification:) withObject: n waitUntilDone: YES];
        
    }
#else
  NSAutoreleasePool * pool = [[NSAutoreleasePool alloc] init];
  
  TNSFileHandleTransport * transport = [[TNSFileHandleTransport alloc] initWithFileHandle: clientSocket];
  id<TProcessor> processor = [mProcessorFactory processorForTransport: transport];
  
  id <TProtocol> inProtocol = [[mInputProtocolFactory newProtocolOnTransport: transport] autorelease];
  id <TProtocol> outProtocol = [[mOutputProtocolFactory newProtocolOnTransport: transport] autorelease];

  @try {
    BOOL result = NO;
    do {
      NSAutoreleasePool * myPool = [[NSAutoreleasePool alloc] init];
      result = [processor processOnInputProtocol: inProtocol outputProtocol: outProtocol];
      [myPool release];
    } while (result);
  }
  @catch (TTransportException * te) {
    //NSLog(@"Caught transport exception, abandoning client connection: %@", te);
  }

  NSNotification * n = [NSNotification notificationWithName: kTSocketServer_ClientConnectionFinishedForProcessorNotification
                                                     object: self
                                                   userInfo: [NSDictionary dictionaryWithObjectsAndKeys: 
                                                              processor,
                                                              kTSocketServer_ProcessorKey,
                                                              transport,
                                                              kTSockerServer_TransportKey,
                                                              nil]];
  [[NSNotificationCenter defaultCenter] performSelectorOnMainThread: @selector(postNotification:) withObject: n waitUntilDone: YES];
  
  [pool release];
#endif
}



@end



