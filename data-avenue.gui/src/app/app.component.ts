import {MessageService} from './message.service';
import {Component, OnInit} from '@angular/core';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent implements OnInit {
  title = 'Data Avenue GUI2';

  constructor(private messageService: MessageService) {
  }

  ngOnInit(): void {
    this.messageService.add('Welcome to Data Avenue GUI2!', true);
  }

  public log(txt: string) {
    console.log(txt);
  }
}
