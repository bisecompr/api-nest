import { Controller, Get, Param, Query } from '@nestjs/common';
import { DatabricksService } from './databricks.service';
import { ApiQuery } from '@nestjs/swagger';

@Controller('databricks')
export class DatabricksController {
  constructor(private readonly databricksService: DatabricksService) { }

  @Get('metrics')
  @ApiQuery({ name: 'campaignName', required: false, type: String, description: 'Filter by campaign name' })
  @ApiQuery({ name: 'startDate', required: false, type: String, description: 'Filter from start date (YYYY-MM-DD)' })
  @ApiQuery({ name: 'endDate', required: false, type: String, description: 'Filter up to end date (YYYY-MM-DD)' })
  async getMetrics(@Query('campaignName') campaignName?: string, @Query('startDate') startDate?: string, @Query('endDate') endDate?: string) {
    return await this.databricksService.getMetrics(campaignName, startDate, endDate)
  }
}
