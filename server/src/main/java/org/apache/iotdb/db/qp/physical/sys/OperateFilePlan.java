/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.qp.physical.sys;

import java.io.File;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.read.common.Path;

public class OperateFilePlan extends PhysicalPlan {

  private File file;
  private File targetDir;
  private boolean autoCreateSchema;
  private int sgLevel;

  public OperateFilePlan(File file, OperatorType operatorType) {
    super(false, operatorType);
    this.file = file;
  }

  public OperateFilePlan(File file, OperatorType operatorType, boolean autoCreateSchema, int sgLevel) {
    super(false, operatorType);
    this.file = file;
    this.autoCreateSchema = autoCreateSchema;
    this.sgLevel = sgLevel;
  }

  public OperateFilePlan(File file, File targetDir, OperatorType operatorType) {
    super(false, operatorType);
    this.file = file;
    this.targetDir = targetDir;
  }

  @Override
  public List<Path> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public List<String> getPathsStrings() {
    return Collections.emptyList();
  }

  public File getFile() {
    return file;
  }

  public File getTargetDir() {
    return targetDir;
  }

  public boolean isAutoCreateSchema() {
    return autoCreateSchema;
  }

  public int getSgLevel() {
    return sgLevel;
  }

  @Override
  public String toString() {
    return "OperateFilePlan{" +
        "file=" + file +
        ", targetDir=" + targetDir +
        ", autoCreateSchema=" + autoCreateSchema +
        ", sgLevel=" + sgLevel +
        ", operatorType=" + getOperatorType() +
        '}';
  }
}
