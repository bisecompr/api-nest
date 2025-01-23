import { Module } from '@nestjs/common';
import { DatabricksModule } from './databricks/databricks.module';

@Module({
  imports: [DatabricksModule],
  controllers: [],
  providers: [],
})
export class AppModule {}
