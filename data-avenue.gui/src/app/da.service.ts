import {Auth} from './auth';
import {Item} from './item';
import {MessageService} from './message.service';
import {Msg} from './msg';
import {TransferStatus} from './transferstatus';
import {Injectable} from '@angular/core';
import {Observable} from 'rxjs/Observable';
import {of} from 'rxjs/observable/of';
import {HttpClient, HttpErrorResponse, HttpHeaders} from '@angular/common/http';
import {catchError} from 'rxjs/operators';
import {throwError} from "rxjs";

@Injectable()
export class DaService {
  C_BROWSER_MODE = 'browsermode';
  C_BASE_URL = 'baseurl';
  C_AUTH_KEY = 'authkey';
  C_ADVANCED_MODE = 'advancedmode';

  daBaseUrl = '/dataavenue/';
  daAuthKey = 'dataavenue-key';
  daAdvancedMode = false;

  enablePanels = true;

  authList: Auth[] = [{type: '', displayName: '', fields: [], url: '', xcredentials: '', isAuthenticated: false},
    {type: '', displayName: '', fields: [], url: '', xcredentials: '', isAuthenticated: false}]; // [side]

  constructor(
    private http: HttpClient,
    private messageService: MessageService) {

    const urlParams = this.getUrlParams(location.href);
    if (urlParams[this.C_BASE_URL] != null) {
      const pDaBaseUrl : string = urlParams[this.C_BASE_URL];
      console.log("url parameter config - baseurl: :" + pDaBaseUrl);
      if (pDaBaseUrl.endsWith('/')){
        this.daBaseUrl = pDaBaseUrl;
      } else {
        this.daBaseUrl = pDaBaseUrl + '/';
      }
    }
  }

  getVersion(): Observable<string | {}> {
    return this.http.get(this.daBaseUrl + 'rest/version', {responseType: 'text'})
      .pipe(catchError(this.handleError2));
  }

  getProtocols(): Observable<Array<string>> {
    const httpOptions = {
      headers: new HttpHeaders()
        .set('x-key', this.daAuthKey)
    };
    return this.http.get<string[]>(this.daBaseUrl + 'rest/protocols', httpOptions)
      .pipe(catchError(this.handleError2));
  }

  getAuth(protocol: string): Observable<Auth[]> {
    const pheaders = new HttpHeaders()
      .set('Content-type', 'application/json')
      .set('x-key', this.daAuthKey);
    const httpOptions = {
      headers: pheaders,
    };

    return this.http.get<Auth[]>(this.daBaseUrl + 'rest/authentication/' + protocol, httpOptions)
      .pipe(catchError(this.handleError2));
  }

  getDirectoryList(url: string, auth: Auth): Observable<Array<string>> {
    this.log('get directory: ' + url);

    const httpOptions = {
      headers: new HttpHeaders()
        .set('x-key', this.daAuthKey)
        .set('x-credentials', auth.xcredentials)
        .set('x-uri', url)
    };

    return this.http.get<string[]>(this.daBaseUrl + 'rest/directory', httpOptions)
      .pipe(catchError(this.handleError2));
  }

  getDirectoryDetailedList(url: string, auth: Auth): Observable<Array<Item>> {
    this.log('get detailed directory: ' + url);

    const httpOptions = {
      headers: new HttpHeaders()
        .set('Content-type', 'application/json')
        .set('x-key', this.daAuthKey)
        .set('x-credentials', auth.xcredentials)
        .set('x-uri', url)
    };

    return this.http.post<Item[]>(this.daBaseUrl + 'rest/attributes', '', httpOptions)
      .pipe(catchError(this.handleError2));
  }

  makeDirectory(url: string, auth: Auth): Observable<Array<string>> {
    this.log('make directory: ' + url);

    const httpOptions = {
      headers: new HttpHeaders()
        .set('x-key', this.daAuthKey)
        .set('x-credentials', auth.xcredentials)
        .set('x-uri', url)
    };

    return this.http.post<string[]>(this.daBaseUrl + 'rest/directory', '', httpOptions)
      .pipe(catchError(this.handleError('make directory', [])));
  }

  deleteDirectory(url: string, auth: Auth): Observable<Array<string>> {
    this.log('delete directory: ' + url);

    const httpOptions = {
      headers: new HttpHeaders()
        .set('x-key', this.daAuthKey)
        .set('x-credentials', auth.xcredentials)
        .set('x-uri', url)
    };

    return this.http.delete<string[]>(this.daBaseUrl + 'rest/directory', httpOptions)
      .pipe(catchError(this.handleError('delete directory', [])));
  }

  deleteFile(url: string, auth: Auth): Observable<Array<string>> {
    this.log('delete file: ' + url);

    const httpOptions = {
      headers: new HttpHeaders()
        .set('x-key', this.daAuthKey)
        .set('x-credentials', auth.xcredentials)
        .set('x-uri', url)
    };

    return this.http.delete<string[]>(this.daBaseUrl + 'rest/file', httpOptions)
      .pipe(catchError(this.handleError('delete file', [])));
  }

  copy(fromUrl: string, toUrl: string, fromAuth: Auth, toAuth: Auth, move: boolean): Observable<Array<string>> {
    let moveParam = '';
    if (move) {
      this.logTyp('move: ' + fromUrl + ' to ' + toUrl, Msg.SUCCESS);
      moveParam = ', move: true';
    } else {
      this.logTyp('copy: ' + fromUrl + ' to ' + toUrl, Msg.SUCCESS);
    }

    const pheaders = new HttpHeaders()
      .set('Content-type', 'application/json')
      .set('responseType', 'text') // bug miatt: https://github.com/angular/angular/issues/18680
      .set('x-key', this.daAuthKey)
      .set('x-credentials', fromAuth.xcredentials)
      .set('x-uri', fromUrl);
    const httpOptions = {
      headers: pheaders,
      responseType: 'string' as 'json'
    };
    const target = '{target:\'' + toUrl + '\', ' + 'credentials:' + toAuth.xcredentials + '' + moveParam + '}';

    return this.http.post<string[]>(this.daBaseUrl + 'rest/transfers', target, httpOptions)
      .pipe(catchError(this.handleError2));
  }

  copyStatus(transferId: string): Observable<TransferStatus> {
    const pheaders = new HttpHeaders()
      .set('Content-type', 'application/json')
      .set('x-key', this.daAuthKey);
    const httpOptions = {
      headers: pheaders,
    };

    return this.http.get<TransferStatus>(this.daBaseUrl + 'rest/transfers/' + transferId, httpOptions)
      .pipe(catchError(this.handleError2));
  }

  copyAbort(transferId: string): Observable<Array<string>> {
    const pheaders = new HttpHeaders()
      .set('Content-type', 'application/json')
      .set('x-key', this.daAuthKey);
    const httpOptions = {
      headers: pheaders,
    };

    return this.http.delete<string[]>(this.daBaseUrl + 'rest/transfers/' + transferId, httpOptions)
      .pipe(catchError(this.handleError2));
  }

  copyAcknowledge(transferId: string): Observable<Array<string>> {
    const pheaders = new HttpHeaders()
      .set('Content-type', 'application/json')
      .set('x-key', this.daAuthKey);
    const httpOptions = {
      headers: pheaders,
    };

    return this.http.put<string[]>(this.daBaseUrl + 'rest/transfers/' + transferId, null, httpOptions)
      .pipe(catchError(this.handleError2));
  }

  getTransfers(): Observable<Array<TransferStatus>> {
    const pheaders = new HttpHeaders()
      .set('Content-type', 'application/json')
      .set('x-key', this.daAuthKey);
    const httpOptions = {
      headers: pheaders,
    };

    return this.http.get<Array<TransferStatus>>(this.daBaseUrl + 'rest/transfers/', httpOptions)
      .pipe(catchError(this.handleError2));
  }

  download(downloadFileUrl: string, fromAuth: Auth) {

    const pheaders = new HttpHeaders()
      .set('Content-type', 'application/octet-stream')
      .set('responseType', 'blob')
      .set('x-key', this.daAuthKey)
      .set('x-credentials', fromAuth.xcredentials)
      .set('x-uri', downloadFileUrl);
    const httpOptions = {
      headers: pheaders,
      responseType: 'blob' as 'json'
    };

    return this.http.get(this.daBaseUrl + 'rest/file', httpOptions)
      .pipe(catchError(this.handleError2));
  }

  downloadUrl(url: string, auth: Auth): Observable<Array<string>> {
    this.log('create download url for: ' + url);

    const httpOptions = {
      headers: new HttpHeaders()
        .set('responseType', 'text') // bug miatt: https://github.com/angular/angular/issues/18680
        .set('x-key', this.daAuthKey)
        .set('x-credentials', auth.xcredentials)
        .set('x-uri', url),
      responseType: 'string' as 'json'
    };

    return this.http.post<string[]>(this.daBaseUrl + 'rest/resourcesession', '', httpOptions)
      .pipe(catchError(this.handleError2));
  }

  /**
   * use the returned ID of downloadUrl
   */
  getdownloadUrl(resourceSessionId: string): string {
    return this.daBaseUrl + 'rest/resourcesession/' + resourceSessionId;
  }

  /**
   * Handle Http operation that failed.
   * Let the app continue by returning an empty result.
   * @param operation - name of the operation that failed
   * @param result - optional value to return as the observable result
   */
  private handleError<T>(operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {
      console.error(error);
      this.logTyp(`${operation} failed: ${error.message}. ${error.error}`, Msg.DANGER);

      return of(result as T);
    };
  }

  private handleError2(error: HttpErrorResponse) {
    if (error.error instanceof ErrorEvent) {
      console.log('A client-side or network error occurred:' + error.error.message);
      return throwError('A client-side or network error occurred:' + error.error.message);
    } else {
      console.log(`Backend returned code ${error.status}, message: ${error.message}, error: ${error.error}`);
      return throwError(` ${error.error} Error code ${error.status}`);
    }
  }

  /**
   * https://www.malcontentboffin.com/2016/11/TypeScript-Function-Decodes-URL-Parameters.html
   * @param url
   */
  getUrlParams(url?: string): any {
    const params = {};
    const splitted = url.split("?", 2);
    if (splitted.length == 2) {
      const urlParams = splitted[1].split("&");
      urlParams.forEach((indexQuery: string) => {
        const keyValue = indexQuery.split("=");
        const key = decodeURIComponent(keyValue[0]);
        const value = decodeURIComponent(keyValue.length > 1 ? keyValue[1] : "");
        params[key] = value;
      });
    }
    return params;
  }

  private log(message: string) {
    this.messageService.add('DA Service: ' + message, this.daAdvancedMode);
  }

  private logTyp(message: string, typ: string) {
    this.messageService.addTyp('DA Service: ' + message, typ, this.daAdvancedMode);
  }
}
