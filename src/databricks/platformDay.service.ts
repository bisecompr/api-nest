import { HttpException, HttpStatus, Injectable } from '@nestjs/common';
import { DatabricksConnectionProvider } from './databricks.provider';
import knex from 'knex';
import { CASE_WHEN_NOME_INTERNO_CAMPANHA } from '../utils/query';

@Injectable()
export class PlatformDayService {
  constructor(private readonly connectionProvider: DatabricksConnectionProvider) { }
  private knexBuilder = knex({
    client: 'pg',
    useNullAsDefault: true,
    wrapIdentifier: (value, origImpl) => `\`${value}\``
  })

  async getMetaEngagement(campaignName?: string, startDate?: string, endDate?: string) {
    let query = this.knexBuilder
      .select([
        this.knexBuilder.raw(CASE_WHEN_NOME_INTERNO_CAMPANHA),
        'actions',
        'video_views',
      ])
      .from('main.plataformas.plataformas_meta')
    try {
      if (!!campaignName) {
        query.where('Nome_Interno_Campanha', campaignName)
      }

      if (!!startDate && !!endDate) {
        query.whereBetween('date', [startDate, endDate])
      } else {
        const today = new Date()
        let lastWeek = new Date()
        lastWeek.setDate(today.getDate() - 7)
        const todayFormatted = today.toISOString().split('T')[0];
        const lastWeekFormatted = lastWeek.toISOString().split('T')[0];

        query.whereBetween('date', [lastWeekFormatted, todayFormatted])
      }
      const stringQuery = query.toString()
      const response = await this.connectionProvider.executeQuery(stringQuery)
      response.map(el => {
        el['actions'] = JSON.parse(el['actions'])
      })
      const filteredCampaigns = response.filter(campaign =>
        campaign.actions?.some(action =>
          action.action_type === "post_reaction" || action.action_type === "comment"
        )
      )
      const result = filteredCampaigns.map((campaign) => {
        const nomeInternoCampanha = campaign['Nome_Interno_Campanha']
        const postReactionObject = campaign?.actions.filter(action => action?.action_type === 'post_reaction')
        const likes = postReactionObject.length > 0 ? postReactionObject[0]['value'] : null
        const commentObject = campaign?.actions.filter(action => action?.action_type === 'comment')
        const comment = commentObject.length > 0 ? commentObject[0]['value'] : null
        const views = campaign['video_views']
        return {
          nomeInternoCampanha,
          likes,
          comment,
          views
        }
      })
      const aggregatedData = Object.values(this.aggregateCampaignData(result))

      return aggregatedData
    } catch (err) {
      console.error(err)
    }
  }

  async getPlatformMetrics(campaignName?: string, startDate?: string, endDate?: string) {
    try {
      let query = this.knexBuilder
        .select('platform')
        .sum({ spend: 'metrics_cost' })
        .sum({ impressions: 'metrics_impressions' })
        .sum({ clicks: 'metrics_clicks' })
        .sum({ engagement: 'metrics_engagement' })
        .sum({ 'video_views_total': 'metrics_video_views' })
        .sum({ 'video_view_25': 'metrics_video_25p' })
        .sum({ 'video_view_50': 'metrics_video_50p' })
        .sum({ 'video_view_75': 'metrics_video_75p' })
        .sum({ 'video_view_100': 'metrics_video_100p' })
        .sum({ conversions_value: 'metrics_conversions_value' })
        .from('main.plataformas.plataformas_dia')
        .groupBy(['platform'])
        .orderBy('platform')

      if (campaignName) {
        query
          .select('Nome_Interno_Campanha')
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
      const result = await this.connectionProvider.executeQuery(query.toString())

      const enrichedResult = result.map((row) => {
        const { spend, impressions, clicks, video_views_total, video_view_100 } = row;

        const safeSpend = spend || 0
        const safeImpressions = impressions || 0
        const safeClicks = clicks || 0
        const safeVideoViews = video_views_total || 0
        const safeMetricsVideo100 = video_view_100 || 0

        const CPM = safeImpressions > 0 ? (safeSpend / (safeImpressions / 1000)) : null
        const CPV = safeVideoViews > 0 ? (safeSpend / safeVideoViews) : null
        const CPC = safeClicks > 0 ? (safeSpend / safeClicks) : null
        const CTR = safeImpressions > 0 ? ((safeClicks / safeImpressions) * 100) : null
        const VTR = safeVideoViews > 0 ? (safeMetricsVideo100 / safeVideoViews) : null

        return {
          ...row,
          CPM,
          CPV,
          CPC,
          CTR,
          VTR
        };
      });

      return enrichedResult.map(item => {
        item['platform'] = item['platform'].charAt(0).toUpperCase() + item['platform'].slice(1)
        return item
      })
    } catch (err) {
      throw new HttpException(`Não foi possível buscar as informações, erro: ${err}`, HttpStatus.BAD_REQUEST)
    }

  }

  async getMetrics(campaignName?: string, startDate?: string, endDate?: string) {

    let baseQuery = this.knexBuilder
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

    if (!!startDate && !!endDate) {
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
    return this.connectionProvider.executeQuery(queryString)
  }

  async getMetricsSegmentedByDate(campaignName?, startDate?: string, endDate?: string) {
    try {
      if (!startDate || !endDate) {
        return await this.getChartByWeekDayForLastWeek(campaignName)

      } else {
        const endDateDated = new Date(endDate)
        const endDateString = endDateDated.toISOString().split('T')[0]
        const startDateDated = new Date(startDate)
        let startDateString = startDateDated.toISOString().split('T')[0]

        const daysOffset = (endDateDated.getTime() - new Date(startDate).getTime()) / (1000 * 60 * 60 * 24)
        if (daysOffset <= 7) {
          return this.getChartByWeekDay(startDate, endDate, campaignName)
        }
        if (daysOffset <= 30) {
          return this.getChartByWeekStartAndEndDay(startDate, endDate, campaignName)
        }
        if (daysOffset > 30) {
          return this.getChartByMonth(startDate, endDate, campaignName)
        }
      }
    } catch (err) {
      throw new HttpException(`${err}`, HttpStatus.BAD_REQUEST)
    }
  }
  private aggregateCampaignData(data) {
    return data.reduce((acc, campaign) => {
      const { nomeInternoCampanha, likes, comment, views } = campaign;
      const key = `${nomeInternoCampanha}`;

      if (!acc[key]) {
        acc[key] = {
          nomeInternoCampanha,
          likes: 0,
          comment: 0,
          views: 0
        };
      }

      acc[key].likes += (likes ?? 0);
      acc[key].comment += (comment ?? 0);
      acc[key].views += (views ?? 0);

      return acc;
    }, {});
  }

  private async getChartByWeekDayForLastWeek(campaignName?: string) {
    const knex = this.knexBuilder
    const today = new Date()
    let yesterday = new Date()
    let lastWeek = new Date()
    yesterday.setDate(today.getDate() - 1)
    lastWeek.setDate(yesterday.getDate() - 13)
    const yesterdayString = yesterday.toISOString().split('T')[0];
    const lastWeekString = lastWeek.toISOString().split('T')[0];
    let baseQuery = knex
      .select([
        'date',
        this.knexBuilder.raw(`
          CASE DAYOFWEEK(date)
            WHEN 1 THEN 'Domingo'
            WHEN 2 THEN 'Segunda-feira'
            WHEN 3 THEN 'Terça-feira'
            WHEN 4 THEN 'Quarta-feira'
            WHEN 5 THEN 'Quinta-feira'
            WHEN 6 THEN 'Sexta-feira'
            WHEN 7 THEN 'Sábado'
          END AS week_day
        `)
      ])
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
      .groupBy("date")
      .orderBy('date', 'desc')
      .whereBetween('date', [lastWeekString, yesterdayString])

    if (!!campaignName) {
      baseQuery.select('Nome_Interno_Campanha')
      baseQuery.groupBy(['Nome_Interno_Campanha', 'date'])
      baseQuery.where('Nome_Interno_Campanha', campaignName)
    }
    const queryString = baseQuery.toString()
    const queryResult = await this.connectionProvider.executeQuery(queryString)
    const middleIndex = queryResult.length / 2
    const actual = queryResult.slice(0, middleIndex)
    const previous = queryResult.slice(middleIndex)

    return { actual, previous }
  }

  private async getChartByWeekDay(startDate: string, endDate: string, campaignName?) {
    const endDateDated = new Date(endDate)
    const startDateDated = new Date(startDate)
    const milisectDiff = endDateDated.getTime() - startDateDated.getTime()
    const lastPeriodStart = new Date(startDateDated.getTime() - milisectDiff)

    let baseQuery = this.knexBuilder
      .select([
        'date',
        this.knexBuilder.raw(`
          CASE DAYOFWEEK(date)
            WHEN 1 THEN 'Domingo'
            WHEN 2 THEN 'Segunda-feira'
            WHEN 3 THEN 'Terça-feira'
            WHEN 4 THEN 'Quarta-feira'
            WHEN 5 THEN 'Quinta-feira'
            WHEN 6 THEN 'Sexta-feira'
            WHEN 7 THEN 'Sábado'
          END AS week_day
        `)
      ])
      .sum({ spend: 'metrics_cost' })
      .sum({ impressions: 'metrics_impressions' })
      .sum({ clicks: 'metrics_clicks' })
      .sum({ engagement: 'metrics_engagement' })
      .sum({ video_views_total: 'metrics_video_views' })
      .sum({ video_view_25: 'metrics_video_25p' })
      .sum({ video_view_50: 'metrics_video_50p' })
      .sum({ video_view_75: 'metrics_video_75p' })
      .sum({ video_view_100: 'metrics_video_100p' })
      .sum({ conversions_value: 'metrics_conversions_value' })
      .from('main.plataformas.plataformas_dia')
      .whereBetween('date', [lastPeriodStart, endDateDated])
      .groupBy("date")
      .orderBy('date', 'desc')
    if (campaignName) {
      baseQuery.select('Nome_Interno_Campanha')
      baseQuery.groupBy(['Nome_Interno_Campanha', 'date'])
      baseQuery.where('Nome_Interno_Campanha', campaignName)
    }

    const queryString = baseQuery.toString()
    const queryResult = await this.connectionProvider.executeQuery(queryString)
    const middleIndex = queryResult.length / 2
    const actual = queryResult.slice(0, middleIndex)
    const previous = queryResult.slice(middleIndex)

    return { actual, previous }
  }

  private async getChartByWeekStartAndEndDay(startDate: string, endDate: string, campaignName?) {
    const endDateDated = new Date(endDate)
    const startDateDated = new Date(startDate)
    const milisectDiff = endDateDated.getTime() - startDateDated.getTime()
    const lastPeriodStart = new Date(startDateDated.getTime() - milisectDiff)

    let baseQuery = this.knexBuilder
      .select([
        this.knexBuilder.raw('DATE_TRUNC("WEEK", date) AS Data_Inicio'),
        this.knexBuilder.raw('DATE_ADD(DATE_TRUNC("WEEK", date), 6) AS Data_Fim')
      ])
      .sum({ spend: 'metrics_cost' })
      .sum({ impressions: 'metrics_impressions' })
      .sum({ clicks: 'metrics_clicks' })
      .sum({ engagement: 'metrics_engagement' })
      .sum({ video_views_total: 'metrics_video_views' })
      .sum({ video_view_25: 'metrics_video_25p' })
      .sum({ video_view_50: 'metrics_video_50p' })
      .sum({ video_view_75: 'metrics_video_75p' })
      .sum({ video_view_100: 'metrics_video_100p' })
      .sum({ conversions_value: 'metrics_conversions_value' })
      .from('main.plataformas.plataformas_dia')
      .whereBetween('date', [lastPeriodStart, endDateDated])
      .groupBy(["Data_Inicio", "Data_Fim"])
      .orderBy('Data_Inicio', 'desc');

    if (campaignName) {
      baseQuery.select('Nome_Interno_Campanha')
      baseQuery.groupBy(['Nome_Interno_Campanha', 'date'])
      baseQuery.where('Nome_Interno_Campanha', campaignName)
    }

    const queryString = baseQuery.toString()
    const queryResult = await this.connectionProvider.executeQuery(queryString)
    const middleIndex = queryResult.length / 2
    const actual = queryResult.slice(0, middleIndex)
    const previous = queryResult.slice(middleIndex)

    return { actual, previous }
  }

  private async getChartByMonth(startDate: string, endDate: string, campaignName?) {
    const endDateDated = new Date(endDate)
    const startDateDated = new Date(startDate)
    const milisectDiff = endDateDated.getTime() - startDateDated.getTime()
    const lastPeriodStart = new Date(startDateDated.getTime() - milisectDiff)

    let baseQuery = this.knexBuilder
      .select([
        this.knexBuilder.raw('DATE_TRUNC("MONTH", date) AS Data_Inicio_Mes'),
        this.knexBuilder.raw('LAST_DAY(DATE_TRUNC("MONTH", date)) AS Data_Fim_Mes')
      ])
      .sum({ spend: 'metrics_cost' })
      .sum({ impressions: 'metrics_impressions' })
      .sum({ clicks: 'metrics_clicks' })
      .sum({ engagement: 'metrics_engagement' })
      .sum({ video_views_total: 'metrics_video_views' })
      .sum({ video_view_25: 'metrics_video_25p' })
      .sum({ video_view_50: 'metrics_video_50p' })
      .sum({ video_view_75: 'metrics_video_75p' })
      .sum({ video_view_100: 'metrics_video_100p' })
      .sum({ conversions_value: 'metrics_conversions_value' })
      .from('main.plataformas.plataformas_dia')
      .whereBetween('date', [lastPeriodStart, endDateDated])
      .groupBy(["Data_Inicio_Mes", "Data_Fim_Mes"])
      .orderBy('Data_Inicio_Mes', 'desc')

    if (campaignName) {
      baseQuery.select('Nome_Interno_Campanha')
      baseQuery.groupBy(['Nome_Interno_Campanha', 'Data_Inicio_Mes', 'Data_Fim_Mes'])
      baseQuery.where('Nome_Interno_Campanha', campaignName)
    }

    const queryString = baseQuery.toString()
    const queryResult = await this.connectionProvider.executeQuery(queryString)
    const middleIndex = queryResult.length / 2
    const actual = queryResult.slice(0, middleIndex)
    const previous = queryResult.slice(middleIndex)

    return { actual, previous }
  }
} 
