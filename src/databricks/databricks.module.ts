import { Module } from '@nestjs/common';
import { PlatformDayService } from './platformDay.service';
import { PlatformDayController } from './platformDay.controller';
import { DatabricksConnectionProvider } from './databricks.provider';

@Module({
  providers: [PlatformDayService, DatabricksConnectionProvider],
  controllers: [PlatformDayController]
})
export class DatabricksModule { }
