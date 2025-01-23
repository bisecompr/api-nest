import { Module } from '@nestjs/common';
import { DatabricksService } from './databricks.service';
import { DatabricksController } from './databricks.controller';
import { DatabricksConnectionProvider } from './databricks.provider';

@Module({
  providers: [DatabricksService, DatabricksConnectionProvider],
  controllers: [DatabricksController]
})
export class DatabricksModule { }
