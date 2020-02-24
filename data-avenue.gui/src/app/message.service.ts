import {Msg} from './msg';
import {Injectable} from '@angular/core';
import {NotificationsService} from 'angular2-notifications';

@Injectable()
export class MessageService {
  messages: Msg[] = [];

  constructor(private _service: NotificationsService) {
  }

  add(message: string, keepLog: boolean) {
    if (keepLog) {
      this.messages.push({typ: Msg.INFO, txt: message});
    }
  }

  addTyp(message: string, typ: string, keepLog: boolean) {
    if (keepLog) {
      this.messages.push({typ: typ, txt: message});
    }
    switch (typ) {
      case Msg.DANGER:
        this._service.error('', message);
        break;
      case Msg.SUCCESS:
        this._service.success('', message, {
          timeOut: 10000,
          showProgressBar: false,
          pauseOnHover: true,
          clickToClose: true
        });
        break;
      case Msg.WARNING:
        this._service.alert('', message, {
          timeOut: 10000,
          showProgressBar: false,
          pauseOnHover: true,
          clickToClose: true
        });
        break;
    }
  }

  clear() {
    this.messages = [];
    this._service.remove();
  }

  clearNotifications() {
    this._service.remove();
  }
}
