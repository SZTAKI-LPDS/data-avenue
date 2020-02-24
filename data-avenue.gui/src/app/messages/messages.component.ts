import {Component, OnInit, ViewChild, HostListener, ElementRef} from '@angular/core';
import {MessageService} from '../message.service';

@Component({
  selector: 'app-messages',
  templateUrl: './messages.component.html',
  styleUrls: ['./messages.component.css']
})

export class MessagesComponent implements OnInit {
  hideLogs = false;

  @ViewChild('scrollMe', {static: false}) scrollMe: ElementRef;


  constructor(public messageService: MessageService) {
    this.onResize();
  }

  ngOnInit() {
  }

  @HostListener('window:resize', ['$event'])
  onResize(event?) {
    try {
      console.log('window.innerHeight:' + window.innerHeight);
    } catch (e) {
      console.log(e);
    }
  }

}
