// SyncWave native capture helper.
// Reads an input audio device (default "BlackHole 2ch") with Core Audio's AudioQueue
// and writes raw 48 kHz stereo s16le PCM to stdout in 10 ms blocks — gap-free.
//
// Build: swiftc -O native/capture.swift -o bin/capture
// Usage: bin/capture ["Device Name"]

import AudioToolbox
import CoreAudio
import Foundation

setvbuf(stdout, nil, _IONBF, 0)
signal(SIGPIPE, SIG_DFL)

let wanted = CommandLine.arguments.count > 1 ? CommandLine.arguments[1] : "BlackHole 2ch"

func stringProp(_ id: AudioObjectID, _ selector: AudioObjectPropertySelector) -> String? {
    var addr = AudioObjectPropertyAddress(mSelector: selector,
                                          mScope: kAudioObjectPropertyScopeGlobal,
                                          mElement: kAudioObjectPropertyElementMain)
    var value: Unmanaged<CFString>?
    var size = UInt32(MemoryLayout<Unmanaged<CFString>?>.size)
    let st = AudioObjectGetPropertyData(id, &addr, 0, nil, &size, &value)
    guard st == noErr, let v = value else { return nil }
    return v.takeRetainedValue() as String
}

func findDeviceUID(named name: String) -> String? {
    var addr = AudioObjectPropertyAddress(mSelector: kAudioHardwarePropertyDevices,
                                          mScope: kAudioObjectPropertyScopeGlobal,
                                          mElement: kAudioObjectPropertyElementMain)
    var size: UInt32 = 0
    guard AudioObjectGetPropertyDataSize(AudioObjectID(kAudioObjectSystemObject), &addr, 0, nil, &size) == noErr else { return nil }
    var ids = [AudioDeviceID](repeating: 0, count: Int(size) / MemoryLayout<AudioDeviceID>.size)
    guard AudioObjectGetPropertyData(AudioObjectID(kAudioObjectSystemObject), &addr, 0, nil, &size, &ids) == noErr else { return nil }
    for id in ids where stringProp(id, kAudioObjectPropertyName) == name {
        return stringProp(id, kAudioDevicePropertyDeviceUID)
    }
    return nil
}

guard let uid = findDeviceUID(named: wanted) else {
    FileHandle.standardError.write("capture: device \"\(wanted)\" not found\n".data(using: .utf8)!)
    exit(2)
}

var format = AudioStreamBasicDescription(
    mSampleRate: 48000,
    mFormatID: kAudioFormatLinearPCM,
    mFormatFlags: kLinearPCMFormatFlagIsSignedInteger | kLinearPCMFormatFlagIsPacked,
    mBytesPerPacket: 4, mFramesPerPacket: 1, mBytesPerFrame: 4,
    mChannelsPerFrame: 2, mBitsPerChannel: 16, mReserved: 0)

let callback: AudioQueueInputCallback = { _, queue, buffer, _, _, _ in
    let b = buffer.pointee
    if b.mAudioDataByteSize > 0 {
        let n = fwrite(b.mAudioData, 1, Int(b.mAudioDataByteSize), stdout)
        if n == 0 { exit(0) }
    }
    AudioQueueEnqueueBuffer(queue, buffer, 0, nil)
}

var queue: AudioQueueRef?
var st = AudioQueueNewInput(&format, callback, nil, nil, nil, 0, &queue)
guard st == noErr, let q = queue else {
    FileHandle.standardError.write("capture: AudioQueueNewInput failed (\(st))\n".data(using: .utf8)!)
    exit(3)
}

var cfUID = uid as CFString
st = AudioQueueSetProperty(q, kAudioQueueProperty_CurrentDevice, &cfUID, UInt32(MemoryLayout<CFString>.size))
guard st == noErr else {
    FileHandle.standardError.write("capture: could not select device (\(st))\n".data(using: .utf8)!)
    exit(4)
}

for _ in 0..<12 {
    var buf: AudioQueueBufferRef?
    AudioQueueAllocateBuffer(q, 480 * 4, &buf) // 10 ms
    if let buf = buf { AudioQueueEnqueueBuffer(q, buf, 0, nil) }
}

st = AudioQueueStart(q, nil)
guard st == noErr else {
    FileHandle.standardError.write("capture: AudioQueueStart failed (\(st))\n".data(using: .utf8)!)
    exit(5)
}
FileHandle.standardError.write("capture: streaming \"\(wanted)\" (\(uid))\n".data(using: .utf8)!)

dispatchMain()
