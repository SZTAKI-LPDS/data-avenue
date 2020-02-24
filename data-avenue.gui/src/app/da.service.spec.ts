import {inject, TestBed} from '@angular/core/testing';

import {DaService} from './da.service';

describe('DaService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [DaService]
    });
  });

  it('should be created', inject([DaService], (service: DaService) => {
    expect(service).toBeTruthy();
  }));
});
