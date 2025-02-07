import { Controller, Get, Param, Query } from '@nestjs/common';
import { PlatformDayService } from './platformDay.service';
import { ApiQuery } from '@nestjs/swagger';

@Controller('plataforma_dia')
export class PlatformDayController {
  constructor(private readonly platformDayService: PlatformDayService) { }

  @Get('campaigns')
  @ApiQuery({ name: 'campaignName', required: false, type: String, description: 'Filter by campaign name' })
  @ApiQuery({ name: 'startDate', required: false, type: String, description: 'Filter from start date (YYYY-MM-DD)' })
  @ApiQuery({ name: 'endDate', required: false, type: String, description: 'Filter up to end date (YYYY-MM-DD)' })
  async getCampaignMetrics(@Query('campaignName') campaignName?: string, @Query('startDate') startDate?: string, @Query('endDate') endDate?: string) {
    return await this.platformDayService.getMetrics(campaignName, startDate, endDate)
  }

  @Get('platform')
  @ApiQuery({ name: 'campaignName', required: false, type: String, description: 'Filter by campaign name' })
  @ApiQuery({ name: 'startDate', required: false, type: String, description: 'Filter from start date (YYYY-MM-DD)' })
  @ApiQuery({ name: 'endDate', required: false, type: String, description: 'Filter up to end date (YYYY-MM-DD)' })
  async getPlatformsMetrics(@Query('campaignName') campaignName?: string, @Query('startDate') startDate?: string, @Query('endDate') endDate?: string) {
    return await this.platformDayService.getPlatformMetrics(campaignName, startDate, endDate)
  }

  @Get('platform/meta/engagement')
  @ApiQuery({ name: 'campaignName', required: false, type: String, description: 'Filter by campaign name' })
  @ApiQuery({ name: 'startDate', required: false, type: String, description: 'Filter from start date (YYYY-MM-DD)' })
  @ApiQuery({ name: 'endDate', required: false, type: String, description: 'Filter up to end date (YYYY-MM-DD)' })
  async getMetaEngagement(@Query('campaignName') campaignName?: string, @Query('startDate') startDate?: string, @Query('endDate') endDate?: string) {
    return await this.platformDayService.getMetaEngagement(campaignName, startDate, endDate)
  }

  @Get('chart')
  @ApiQuery({ name: 'campaignName', required: false, type: String, description: 'Filter by campaign name' })
  @ApiQuery({ name: 'startDate', required: false, type: String, description: 'Filter from start date (YYYY-MM-DD)' })
  @ApiQuery({ name: 'endDate', required: false, type: String, description: 'Filter up to end date (YYYY-MM-DD)' })
  async getMetricsSegmentedByDate(@Query('campaignName') campaignName?: string, @Query('startDate') startDate?: string, @Query('endDate') endDate?: string) {
    return await this.platformDayService.getMetricsSegmentedByDate(campaignName, startDate, endDate)
  }
}
