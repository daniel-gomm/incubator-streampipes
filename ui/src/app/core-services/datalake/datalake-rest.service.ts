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

import { HttpClient, HttpRequest } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { DataLakeMeasure } from '../../core-model/datalake/DataLakeMeasure';
import { DataResult } from '../../core-model/datalake/DataResult';
import { GroupedDataResult } from '../../core-model/datalake/GroupedDataResult';
import { PageResult } from '../../core-model/datalake/PageResult';
import { AuthStatusService } from '../../services/auth-status.service';

@Injectable()
export class DatalakeRestService {

    constructor(private http: HttpClient,
                private authStatusService: AuthStatusService) {
    }

    private get baseUrl() {
        return '/streampipes-backend';
    }

    private get dataLakeUrlV3() {
        return this.baseUrl + '/api/v3/users/' + this.authStatusService.email + '/datalake';
    }

    getAllInfos() {
        return this.http.get<DataLakeMeasure[]>(this.dataLakeUrlV3 + '/info');
    }

    getDataPage(index, itemsPerPage, page) {
        return this.http.get<PageResult>(this.dataLakeUrlV3 + '/data/' + index + '/paging?itemsPerPage=' + itemsPerPage + '&page=' + page);
    }

    getDataPageWithoutPage(index, itemsPerPage) {
        return this.http.get<PageResult>(this.dataLakeUrlV3 + '/data/' + index + '/paging?itemsPerPage=' + itemsPerPage);
    }

    getData(index, startDate, endDate, aggregationTimeUnit, aggregationValue): Observable<DataResult> {
        return this.http.get<DataResult>(this.dataLakeUrlV3 + '/data/' + index + '/' + startDate + '/' + endDate + '?aggregationUnit=' + aggregationTimeUnit + '&aggregationValue=' + aggregationValue);
    }

    getGroupedData(index, startDate, endDate, aggregationTimeUnit, aggregationValue, groupingTag) {
        return this.http.get<GroupedDataResult>(this.dataLakeUrlV3 + '/data/' + index + '/' + startDate + '/' + endDate + '/grouping/' + groupingTag + '?aggregationUnit=' + aggregationTimeUnit + '&aggregationValue=' + aggregationValue);
    }

    getDataAutoAggergation(index, startDate, endDate) {
        return this.http.get<DataResult>(this.dataLakeUrlV3 + '/data/' + index + '/' + startDate + '/' + endDate);
    }

    getGroupedDataAutoAggergation(index, startDate, endDate, groupingTag) {
      return this.http.get<GroupedDataResult>(this.dataLakeUrlV3 + '/data/' + index + '/' + startDate + '/' + endDate + '/grouping/' + groupingTag);
    }

    downloadRowData(index, format) {
        const request = new HttpRequest('GET', this.dataLakeUrlV3 + '/data/' + index + '/download?format=' + format,  {
            reportProgress: true,
            responseType: 'text'
        });
        return this.http.request(request);
    }

    downloadRowDataTimeInterval(index, format, startDate, endDate) {
        const request = new HttpRequest('GET', this.dataLakeUrlV3 + '/data/' + index + '/' + startDate + '/' + endDate + '/download' +
            '?format=' + format, {
            reportProgress: true,
            responseType: 'text'
        });
        return this.http.request(request);
    }

    getLabels() {
        return {
          'boxes': ['blue', 'red'],
          'sign': ['trafficsign'],
          'person': ['person', 'Child'],
          'vehicle': ['bicycle', 'car', 'motorcycle', 'airplane', 'bus', 'train', 'truck', 'boat'],
          'outdoor': ['traffic light', 'fire hydrant', 'stop sign', 'parking meter', 'bench'],
          'animal': ['bird', 'cat', 'dog'],
          'accessory': ['backpack', 'umbrella', 'handbag', 'suitcase'],
          'sports': ['frisbee', 'sports ball', 'skis', 'frisbee', 'baseball bat'],
          'kitchen': ['bottle', 'cup', 'fork', 'knife', 'spoon'],
          'furniture': ['chair', 'couch', 'bed', 'table'],
          'electronic': ['tv', 'laptop', 'mouse', 'keyboard']
        };
    }

    get_timeseries_labels() {
        // mocked labels
        const labels = {
          state: ['online', 'offline', 'active', 'inactive'],
          coffee: ['coffee', 'coffee special', 'espresso', '2x espresso', 'hot water'],
          trend: ['increasing', 'decreasing'],
          daytime: ['day', 'night']};
        return labels;
    }

    getImageUrl(imageRoute) {
      return this.dataLakeUrlV3 + '/data/image/' + imageRoute + '/file';
    }

    getCocoFileForImage(imageRoute) {
      return this.http.get(this.dataLakeUrlV3 + '/data/image/' + imageRoute + '/coco');
    }

    saveCocoFileForImage(imageRoute, data) {
      return this.http.post(this.dataLakeUrlV3 + '/data/image/' + imageRoute + '/coco', data);
    }

    saveLabelsInDatabase(index, startDate, endDate, label) {
        const request = new HttpRequest('POST', this.dataLakeUrlV3 + '/data/' + index + '/' + startDate + '/' +
            endDate + '/labeling?label=' + label,  {}, {
            reportProgress: true,
            responseType: 'text'
        });
        return this.http.request(request);
    }

}
