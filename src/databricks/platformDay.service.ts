import { HttpException, HttpStatus, Injectable } from '@nestjs/common';
import { DatabricksConnectionProvider } from './databricks.provider';
import knex from 'knex';

@Injectable()
export class PlatformDayService {
  constructor(private readonly connectionProvider: DatabricksConnectionProvider) { }
  private knexBuilder = knex({
    client: 'pg',
    useNullAsDefault: true,
    wrapIdentifier: (value, origImpl) => `\`${value}\``
  })

  private baseQuery = this.knexBuilder
    .select('Nome_Interno_Campanha')
    .sum({ spend: 'metrics_cost' })
    .sum({ impressions: 'metrics_impressions' })
    .sum({ clicks: 'metrics_clicks' })
    .sum({ engagement: 'metrics_engagement' })
    .sum({ 'video_views_total': 'metrics_video_views' })
    .sum({ 'video_view_25%': 'metrics_video_25p' })
    .sum({ 'video_view_50%': 'metrics_video_50p' })
    .sum({ 'video_view_75%': 'metrics_video_75p' })
    .sum({ 'video_view_100%': 'metrics_video_100p' })
    .sum({ conversions_value: 'metrics_conversions_value' })
    .from('main.plataformas.plataformas_dia')
    .groupBy('Nome_Interno_Campanha')
    .orderBy('impressions', 'desc')

  async getPlatformMetrics(campaignName?: string, startDate?: string, endDate?: string) {
    try {
      let query = this.knexBuilder
        .select('platform')
        .select('Nome_Interno_Campanha')
        .sum({ spend: 'metrics_cost' })
        .sum({ impressions: 'metrics_impressions' })
        .sum({ clicks: 'metrics_clicks' })
        .sum({ engagement: 'metrics_engagement' })
        .sum({ 'video_views_total': 'metrics_video_views' })
        .sum({ 'video_view_25%': 'metrics_video_25p' })
        .sum({ 'video_view_50%': 'metrics_video_50p' })
        .sum({ 'video_view_75%': 'metrics_video_75p' })
        .sum({ 'video_view_100%': 'metrics_video_100p' })
        .sum({ conversions_value: 'metrics_conversions_value' })
        .from('main.plataformas.plataformas_dia')
        .groupBy(['platform', 'Nome_Interno_Campanha'])
        .orderBy('platform')

      if (campaignName) {
        query
          .groupBy('platform', 'Nome_Interno_Campanha')
          .where('Nome_Interno_Campanha', campaignName)
      }
      if (startDate && endDate) {
        query.whereBetween('date', [startDate, endDate])
      } else {
        const today = new Date()
        let lastWeek = new Date()
        lastWeek.setDate(today.getDate() - 7)
        const todayFormatted = today.toISOString().split('T')[0];
        const lastWeekFormatted = lastWeek.toISOString().split('T')[0];

        query.whereBetween('date', [lastWeekFormatted, todayFormatted])

      }
      return this.connectionProvider.executeQuery(query.toString())
    } catch (err) {
      throw new HttpException(`Não foi possível buscar as informações, erro: ${err}`, HttpStatus.BAD_REQUEST)
    }

  }

  async getMetrics(campaignName?: string, startDate?: string, endDate?: string) {
    let baseQuery = this.baseQuery

    if (startDate && endDate) {
      baseQuery = baseQuery
        .whereBetween('date', [startDate, endDate])
    } else {
      const today = new Date()
      let lastWeek = new Date()
      lastWeek.setDate(today.getDate() - 7)
      const todayFormatted = today.toISOString().split('T')[0];
      const lastWeekFormatted = lastWeek.toISOString().split('T')[0];

      baseQuery = baseQuery
        .whereBetween('date', [lastWeekFormatted, todayFormatted])
    }
    if (campaignName) {
      baseQuery.where('Nome_Interno_Campanha', campaignName)
    }

    const queryString = baseQuery.toString()
    console.log(queryString)
    return this.connectionProvider.executeQuery(queryString)
  }
}
