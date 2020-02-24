import {Auth} from '../auth';
import {DaService} from '../da.service';
import {Component, EventEmitter, HostListener, Input, OnInit, Output} from '@angular/core';
import {NgbActiveModal} from '@ng-bootstrap/ng-bootstrap';

@Component({
  selector: 'app-modal-auth',
  templateUrl: './modal-auth.component.html',
  styleUrls: ['./modal-auth.component.css']
})

export class ModalAuthComponent implements OnInit {
  @Input() currentAuth: Auth[];
  selectedIndex = 0;

  @Input() currentdDir;
  selectedSide;
  @Output() out = new EventEmitter();


  constructor(public activeModal: NgbActiveModal,
              public daService: DaService,
  ) {
  }

  @HostListener('document:keydown', ['$event']) onKeydownHandler(event: KeyboardEvent) {
    switch (event.key) {

      case 'Tab':
        //console.log('AUTH Tab pressed');
        break;
      default:
        // console.log('AUTH pressed:' + event.key);
        break;
    }
  }

  ngOnInit() {
    this.getAuth(this.getProtocolFromUrl(this.currentdDir));
  }

  getAuth(protocol: string) {
    this.daService.getAuth(protocol).subscribe(auth => {
        for (let authIndex = 0; authIndex < auth.length; authIndex++) {
          const authType = auth[authIndex];
          for (let fieldIdex = 0; fieldIdex < authType.fields.length; fieldIdex++) {
            const field = authType.fields[fieldIdex];
            auth[authIndex].fields[fieldIdex].value = field.defaultValue;
            if (auth[authIndex].fields[fieldIdex].type.length === 0) {
              auth[authIndex].fields[fieldIdex].type = 'text';
            }
          }
        }
        this.currentAuth = auth;
      },
      terror => {
        console.log('Get auth error: ' + terror);
      }
    );
  }

  onOk() {
    this.daService.authList[this.selectedSide] = this.currentAuth[this.selectedIndex];

    let xcred = '{type:' + this.currentAuth[this.selectedIndex].type + '';
    for (let index = 0; index < this.currentAuth[this.selectedIndex].fields.length; index++) {
      const field = this.currentAuth[this.selectedIndex].fields[index];
      xcred = xcred + ', ' + field.keyName + ':\'' + field.value + '\'';
    }
    xcred = xcred + '}';

    this.daService.authList[this.selectedSide].xcredentials = xcred;
    this.daService.authList[this.selectedSide].url = this.currentdDir;
    this.daService.authList[this.selectedSide].isAuthenticated = true;
  }

  setSelected(selIndex: number) {
    console.log('selected authentication index: ' + selIndex);
    this.selectedIndex = selIndex;
  }

  getProtocolFromUrl(url: string): string {
    const ddot = url.indexOf(':');
    if (ddot < 0) {
      return null;
    } else {
      return url.substring(0, ddot);
    }
  }
}
