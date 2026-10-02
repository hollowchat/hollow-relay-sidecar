//! A tunnel cannot escape the sidecar's lifetime on Windows. Start suspended,
//! attach to a kill-on-close job, then resume its primary thread. Fail closed.
#[cfg(windows)]
pub fn attach_and_resume(
    child: &tokio::process::Child,
) -> std::io::Result<std::os::windows::io::OwnedHandle> {
    use std::{
        mem::{size_of, zeroed},
        os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
        ptr,
    };
    use windows_sys::Win32::{
        Foundation::INVALID_HANDLE_VALUE,
        System::{
            Diagnostics::ToolHelp::{
                CreateToolhelp32Snapshot, Thread32First, Thread32Next, TH32CS_SNAPTHREAD,
                THREADENTRY32,
            },
            JobObjects::{
                AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
                SetInformationJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
                JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            },
            Threading::{OpenThread, ResumeThread, THREAD_SUSPEND_RESUME},
        },
    };
    // All handles are for a newly spawned child or this process's own job.
    // OwnedHandle closes every resource on all error paths.
    unsafe {
        let raw = CreateJobObjectW(ptr::null(), ptr::null());
        if raw.is_null() {
            return Err(std::io::Error::last_os_error());
        }
        let job = OwnedHandle::from_raw_handle(raw);
        let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = zeroed();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        if SetInformationJobObject(
            raw,
            JobObjectExtendedLimitInformation,
            &limits as *const _ as *const _,
            size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
        ) == 0
        {
            return Err(std::io::Error::last_os_error());
        }
        let process = child
            .raw_handle()
            .ok_or_else(|| std::io::Error::other("Child exited before job assignment"))?;
        if AssignProcessToJobObject(raw, process) == 0 {
            return Err(std::io::Error::last_os_error());
        }
        let snapshot_raw = CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0);
        if snapshot_raw == INVALID_HANDLE_VALUE {
            return Err(std::io::Error::last_os_error());
        }
        let snapshot = OwnedHandle::from_raw_handle(snapshot_raw);
        let mut entry: THREADENTRY32 = zeroed();
        entry.dwSize = size_of::<THREADENTRY32>() as u32;
        let mut present = Thread32First(snapshot.as_raw_handle(), &mut entry);
        while present != 0 {
            if Some(entry.th32OwnerProcessID) == child.id() {
                let thread_raw = OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID);
                if thread_raw.is_null() {
                    return Err(std::io::Error::last_os_error());
                }
                let _thread = OwnedHandle::from_raw_handle(thread_raw);
                if ResumeThread(thread_raw) == u32::MAX {
                    return Err(std::io::Error::last_os_error());
                }
                return Ok(job);
            }
            present = Thread32Next(snapshot.as_raw_handle(), &mut entry);
        }
        Err(std::io::Error::other(
            "Suspended child thread was not found",
        ))
    }
}
