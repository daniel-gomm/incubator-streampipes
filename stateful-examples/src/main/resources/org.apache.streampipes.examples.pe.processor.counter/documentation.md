<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~    http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  ~
  -->
  
<p align="center"> 
    <img src="icon.png" width="150px;" class="pe-image-documentation"/>
</p>

***


A processor that counts the events that arrive. That's it.

***

The input does not matter.

***

A user hat to provide a text parameter because, weirdly enough the processor doesn't work otherwise, even though there is no apparent reason for that.

The processor adds a "count" filed to the events that represents the count of arrived events.
