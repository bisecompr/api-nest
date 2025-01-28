import { Module } from '@nestjs/common';
import { PlatformDayService } from './platformDay.service';
import { PlatformDayController } from './platformDay.controller';
import { PlatformController } from './platform.controller';
import { DatabricksConnectionProvider } from './databricks.provider';
import { PlatformService } from './platform.service';

@Module({
  providers: [PlatformDayService, PlatformService, DatabricksConnectionProvider],
  controllers: [PlatformDayController, PlatformController]
})
export class DatabricksModule { }
