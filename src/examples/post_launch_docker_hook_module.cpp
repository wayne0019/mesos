// Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //     http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.

#include <mesos/hook.hpp>
#include <mesos/mesos.hpp>
#include <mesos/module.hpp>

#include <mesos/module/hook.hpp>

#include <process/future.hpp>
#include <process/process.hpp>
#include <process/protobuf.hpp>
#include <process/check.hpp>
#include <process/collect.hpp>
#include <process/io.hpp>
#include <process/subprocess.hpp>

#include <stout/foreach.hpp>
#include <stout/os.hpp>
#include <stout/try.hpp>
#include "common/status_utils.hpp"

#include "messages/messages.hpp"

using namespace mesos;
using namespace process;

using std::map;
using std::string;

using process::Future;

template <typename T>
static Future<T> failure(
    const string& cmd,
    int status,
    const string& err)
{
  return Failure(
      "Failed to '" + cmd + "': exit status = " +
      WSTRINGIFY(status) + " stderr = " + err);
}

static Future<Nothing> _checkError(const string& cmd, const Subprocess& s)
{
  Option<int> status = s.status().get();
  if (status.isNone()) {
    return Failure("No status found for '" + cmd + "'");
  }

  if (status.get() != 0) {
    // TODO(tnachen): Consider returning stdout as well.
    CHECK_SOME(s.err());
    return io::read(s.err().get())
      .then(lambda::bind(failure<Nothing>, cmd, status.get(), lambda::_1));
  }

  return Nothing();
}


// Returns a failure if no status or non-zero status returned from
// subprocess.
static Future<Nothing> checkError(const string& cmd, const Subprocess& s)
{
  return s.status()
    .then(lambda::bind(_checkError, cmd, s));
}

static Future<Nothing> runCommand(const string cmd)
{
  LOG(INFO) << "Running " << cmd;

  Try<Subprocess> s = subprocess(
      cmd,
      Subprocess::PATH("/dev/null"),
      Subprocess::PATH("/dev/null"),
      Subprocess::PIPE());

  if (s.isError()) {
    return Failure(s.error());
  }

  return checkError(cmd, s.get());
}

class PostLaunchDockerHook : public Hook
{
public:
  PostLaunchDockerHook(
    const string& _cmd)
  : cmd(_cmd){}

  virtual Result<Labels> masterLaunchTaskLabelDecorator(
      const TaskInfo& taskInfo,
      const FrameworkInfo& frameworkInfo,
      const SlaveInfo& slaveInfo)
  {
    LOG(INFO) << "Executing 'masterLaunchTaskLabelDecorator' hook";
    return None();
  }

  virtual Try<Nothing> masterSlaveLostHook(const SlaveInfo& slaveInfo)
  {
    LOG(INFO) << "Executing 'masterSlaveLostHook' in slave '"
              << slaveInfo.id() << "'";
    return Nothing();
  }


  // TODO(nnielsen): Split hook tests into multiple modules to avoid
  // interference.
  virtual Result<Labels> slaveRunTaskLabelDecorator(
      const TaskInfo& taskInfo,
      const ExecutorInfo& executorInfo,
      const FrameworkInfo& frameworkInfo,
      const SlaveInfo& slaveInfo)
  {
    LOG(INFO) << "Executing 'slaveRunTaskLabelDecorator' hook";

    return None();
  }


  // In this hook, we create a new environment variable "FOO" and set
  // it's value to "bar".
  virtual Result<Environment> slaveExecutorEnvironmentDecorator(
      const ExecutorInfo& executorInfo)
  {
    LOG(INFO) << "Executing 'slaveExecutorEnvironmentDecorator' hook";
    return None();
  }


  virtual Try<Nothing> slavePreLaunchDockerHook(
      const ContainerInfo& containerInfo,
      const CommandInfo& commandInfo,
      const Option<TaskInfo>& taskInfo,
      const ExecutorInfo& executorInfo,
      const string& name,
      const string& sandboxDirectory,
      const string& mappedDirectory,
      const Option<Resources>& resources,
      const Option<map<string, string>>& env)
  {
    LOG(INFO) << "Executing 'slavePreLaunchDockerHook'";
    return Nothing();
  }

  virtual Try<Nothing> slavePostLaunchDockerHook(
      const ContainerInfo& containerInfo,
      const CommandInfo& commandInfo,
      const Option<TaskInfo>& taskInfo,
      const ExecutorInfo& executorInfo,
      const string& name,
      const string& sandboxDirectory,
      const string& mappedDirectory,
      const Option<Resources>& resources,
      const Option<map<string, string>>& env)
  {
    LOG(INFO) << "Executing 'slavePostLaunchDockerHook' " + name;
    runCommand(cmd + " " + name);
    return Nothing();
  }


  // This hook locates the file created by environment decorator hook
  // and deletes it.
  virtual Try<Nothing> slaveRemoveExecutorHook(
      const FrameworkInfo& frameworkInfo,
      const ExecutorInfo& executorInfo)
  {
    LOG(INFO) << "Executing 'slaveRemoveExecutorHook'";
    return Nothing();
  }


  virtual Result<TaskStatus> slaveTaskStatusDecorator(
      const FrameworkID& frameworkId,
      const TaskStatus& status)
  {
    LOG(INFO) << "Executing 'slaveTaskStatusDecorator' hook";
    return None();
  }


  virtual Result<Resources> slaveResourcesDecorator(
      const SlaveInfo& slaveInfo)
  {
    LOG(INFO) << "Executing 'slaveResourcesDecorator' hook";
    return None();
  }


  virtual Result<Attributes> slaveAttributesDecorator(
      const SlaveInfo& slaveInfo)
  {
    LOG(INFO) << "Executing 'slaveAttributesDecorator' hook";
    return None();
  }

private:
  const string cmd;
};


static Hook* createHook(const Parameters& parameters)
{
  string cmd = "/usr/local/bin/linkerconfig";
  for (int i = 0; i < parameters.parameter_size(); i++) {
    const Parameter& param = parameters.parameter(i);
    if (param.key() == "cmd") {
      cmd =  param.value();
    }
  }
  return new PostLaunchDockerHook(cmd);
}

// Declares a Hook module named 'org_apache_mesos_TestHook'.
mesos::modules::Module<Hook> org_apache_mesos_PostLaunchDockerHook(
    MESOS_MODULE_API_VERSION,
    MESOS_VERSION,
    "Linker Networks",
    "cliu@linkernetworks.com",
    "PostLaunchDockerHook",
    NULL,
    createHook);