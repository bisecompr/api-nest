import { Controller, Get, Query } from '@nestjs/common';
import { DatabricksService } from './databricks.service';

@Controller('databricks')
export class DatabricksController {
  constructor(private readonly databricksService: DatabricksService) { }

  @Get('query')
  async query(@Query('sql') sql: string) {
    try {
      const results = await this.databricksService.executeQuery(sql);
      return { success: true, data: results };
    } catch (error) {
      return { success: false, error: error.message };
    }
  }
}
