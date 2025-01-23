import { Module } from '@nestjs/common';
import { DatabricksService } from './databricks.service';
import { DatabricksController } from './databricks.controller';

@Module({
  providers: [DatabricksService],
  controllers: [DatabricksController]
})
export class DatabricksModule {}
