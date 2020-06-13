/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

import { Directive, EventEmitter, Injectable, Input, Output } from '@angular/core';
import { EventSchema } from '../../../../../../connect/schema-editor/model/EventSchema';
import { DataResult } from '../../../../../../core-model/datalake/DataResult';
import { GroupedDataResult } from '../../../../../../core-model/datalake/GroupedDataResult';

@Injectable()
@Directive()
export abstract class BaseVisualisationComponent {


    @Input() set datas(value: DataResult | GroupedDataResult) {
        if (value !== undefined) {
            this.data = this.clone(value);
            if (this.data !== undefined && this.xKey !== undefined && this.yKeys !== undefined) {
                this.transform();
                this.display();
            }
        } else {
            this.stopDisplayData();
            this.data = undefined;
        }
    }
    @Input() set xAxesKey(value: string) {
        if (value !== undefined) {
            this.xKey = value;
            if (this.data !== undefined && this.xKey !== undefined && this.yKeys !== undefined) {
                this.transform();
                this.display();
            }
        } else {
            this.stopDisplayData();
            this.xKey = undefined;
        }
    }
    @Input() set yAxesKeys(value: string[]) {
        if (value !== undefined) {
            this.yKeys = value;
            if (this.data !== undefined && this.xKey !== undefined && this.yKeys !== undefined) {
                if (this.transformedData === undefined) {
                    this.transform();
                }
                this.display();
            }
        } else {
            this.stopDisplayData();
            this.yKeys = undefined;
        }
    }

    @Input() eventschema: EventSchema = undefined;

    @Input() startDateData: Date = undefined;
    @Input() endDateData: Date = undefined;


    @Input() currentPage: number = undefined;
    @Input() maxPage: number = undefined;
    @Input() enablePaging = false;

    @Output() previousPage = new EventEmitter<boolean>();
    @Output() nextPage = new EventEmitter<boolean>();
    @Output() firstPage = new EventEmitter<boolean>();
    @Output() lastPage = new EventEmitter<boolean>();

    xKey: string = undefined;
    yKeys: string[] = undefined;

    data: DataResult | GroupedDataResult = undefined;
    transformedData: DataResult | GroupedDataResult = undefined;


    dataMode = '';


    transform() {
        if (this.data['headers'] !== undefined) {
            this.transformedData = this.transformData(this.data as DataResult, this.xKey);
            this.dataMode = 'single';
        } else {
            this.transformedData = this.transformGroupedData(this.data as GroupedDataResult, this.xKey);
            this.dataMode = 'group';
        }
    }

    display() {
        if (this.data['headers'] !== undefined) {
            this.displayData(this.transformedData as DataResult, this.yKeys);
        } else {
            this.displayGroupedData(this.transformedData as GroupedDataResult, this.yKeys);
        }
    }

    // transform the input data to the schema of the chart
    abstract transformData(data: DataResult, xKey: string): DataResult;

    // transform the grouped input data to the schema of the chart
    abstract transformGroupedData(data: GroupedDataResult, xKey: string): GroupedDataResult;

    // display the data
    abstract displayData(transformedData: DataResult, yKeys: string[]);

    // display the grouped data
    abstract displayGroupedData(transformedData: GroupedDataResult, yKeys: string[]);

    //
    abstract stopDisplayData();

    clickPreviousPage() {
        this.previousPage.emit();
    }

    clickNextPage() {
        this.nextPage.emit();
    }

    clickFirstPage() {
        this.firstPage.emit();
    }

    clickLastPage() {
        this.lastPage.emit();
    }

    clone(value): DataResult {
        return (JSON.parse(JSON.stringify(value)));
    }

}
