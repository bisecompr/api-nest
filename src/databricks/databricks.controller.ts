import { Controller, Get, Param } from '@nestjs/common';
import { DatabricksService } from './databricks.service';

@Controller('databricks')
export class DatabricksController {
  constructor(private readonly databricksService: DatabricksService) { }

  @Get('campaigns')
  async getCampaigns() {
    try {
      const campaigns = await this.databricksService.getCampaigns();
      return campaigns
    } catch (error) {
      return error;
    }
  }

  @Get('metrics')
  async getMetrics() {
    try {
      const metrics = await this.databricksService.getAllMetrics()
      return metrics
    } catch (err) {

    }
  }

  @Get('metrics/campaign/:campaignName')
  async getMetricsByCampaign(@Param('campaignName') campaignName: string) {
    try {
      const metrics = await this.databricksService.getCampaignMetrics(campaignName)
      return metrics
    } catch (err) {

    }
  }
}
